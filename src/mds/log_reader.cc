#include "log_reader.h"

#include "common/status.h"
#include "common/crc.h"
#include "common/coding.h"

namespace morph {
namespace mds {
namespace log {

Reader::Reader(SequentialFile *file, bool checksum):
    file(file),
    checksum(checksum),
    backing_store(new char[BLOCK_SIZE]),
    buffer(),
    eof(false),
    last_record_offset(0),
    end_of_buffer_offset(0)
{}

Reader::~Reader() {
  delete[] backing_store;
}

bool Reader::read_record(Slice *record, std::string *scratch) {
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;
  bool in_fragmented_record = false;
  Slice fragment;

  scratch->clear();
  record->clear();

  while (true) {
    const unsigned int record_type = read_physical_block(&fragment);

    // read_physical_record may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    uint64_t physical_record_offset = 
      end_of_buffer_offset - buffer.size() - HEADER_SIZE - fragment.size();

    switch (record_type) {
      case FULL_TYPE:
        if (in_fragmented_record) {
          assert(!scratch->empty());
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        last_record_offset = prospective_record_offset;
        return true;

      case FIRST_TYPE:
        if (in_fragmented_record) {
          assert(scratch->empty());
        }
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case MIDDLE_TYPE:
        if (!in_fragmented_record) {
          assert(false && "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;
      
      case LAST_TYPE:
        if (!in_fragmented_record) {
          assert(false && "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset = prospective_record_offset;
          return true;
        }
        break;

      case FEOF:
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record
          scratch->clear();
        }
        return false;
      
      case BAD_RECORD:
        if (in_fragmented_record) {
          assert(false && "error in middle of record");
        }
      
      default: {
        char buf[40];
        std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        assert(false);  
      }
    }
  }
  return false;
}

unsigned int Reader::read_physical_block(Slice *result) {
  while (true) {
    if (buffer.size() < HEADER_SIZE) {
      if (!eof) {
        // Last read was a full read, so this is a trailer to skip
        buffer.clear();

        // Now we try to read the current block
        Status status = file->read(BLOCK_SIZE, &buffer, backing_store);
        end_of_buffer_offset += buffer.size();

        if (!status.is_ok()) {
          buffer.clear();
          eof = true;
          return FEOF;
        } else if (buffer.size() < BLOCK_SIZE) {
          eof = true;
        }
        continue;
      } else {
        // Note that if buffer is non-empty, we have a truncated header at the
        // end ofthe file, which can be caused by the writer crashing in the 
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer.clear();
        return FEOF;
      }
    }

    const char *header = buffer.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);

    if (HEADER_SIZE + length > buffer.size()) {
      size_t drop_size = buffer.size();
      buffer.clear();
      if (!eof) {
        return BAD_RECORD;
      }
      // If the end of the file has been reached without reading "length" bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return FEOF;
    }

    if (checksum) {
      uint32_t actual_crc = crc32c::unmask(morph::decode_fixed_32(header));
      uint32_t expected_crc = crc32c::crc32_fast(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        size_t drop_size = buffer.size();
        buffer.clear();
        fprintf(stderr, "drop_size %lu checksum mismatched expected %u actual %u\n", 
                drop_size, expected_crc, actual_crc);
        return BAD_RECORD;
      }
    }

    buffer.remove_prefix(HEADER_SIZE + length);

    *result = Slice(header + HEADER_SIZE, length);
    return type;
  }
}


} // namespace log
} // namespace mds
} // namespace morph