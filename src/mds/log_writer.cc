// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors
//
// Code modified.


#include "log_writer.h"

#include <unistd.h>

#include "common/crc.h"
#include "common/coding.h"

namespace morph {
namespace mds {
namespace log {

static void InitTypeCrc(uint32_t *type_crc) {
  for (int i = 0; i <= MAX_RECORD_TYPE; ++i) {
    char t = static_cast<char>(i);
    type_crc[i] = ~crc32c::crc32_fast(&t, 1);
  }
}

Writer::Writer(WritableFile *dest):
    block_offset(0),
    dest(dest) {
  InitTypeCrc(type_crc);
}

Writer::~Writer()
{}

Status Writer::add_record(const Slice &slice) {
  const char *ptr = slice.data();
  size_t left = slice.size();

  Status s;
  bool begin = true;
  do {
    const int leftover = BLOCK_SIZE - block_offset;
    assert(leftover >= 0);
    
    if (leftover < HEADER_SIZE) {
      // Switch to a new block
      if (leftover > 0) {
        static_assert(HEADER_SIZE == 7, "");
        dest->append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block
    assert(BLOCK_SIZE - block_offset - HEADER_SIZE >= 0);

    const size_t avail = BLOCK_SIZE - block_offset - HEADER_SIZE;
    const size_t fragment_length = (left < avail) ? left : avail;  // Length that fit in this block

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = FULL_TYPE;
    } else if (begin) {
      type = FIRST_TYPE;
    } else if (end) {
      type = LAST_TYPE;
    } else {
      type = MIDDLE_TYPE;
    }

    s = emit_physical_record(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.is_ok() && left > 0);

  return s;
}

Status Writer::emit_physical_record(RecordType type, const char *ptr, size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset + HEADER_SIZE + length <= BLOCK_SIZE);

  char buf[HEADER_SIZE];
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  buf[6] = static_cast<char>(type);

  // Compute the crc of the record type and the payload
  uint32_t crc = crc32c::crc32_fast(type_crc[type], ptr, length);
  crc = crc32c::mask(crc);
  encode_fixed_32(buf, crc);

  // Write the header and the payload
  Status s = dest->append(Slice(buf, HEADER_SIZE));
  if (s.is_ok()) {
    s = dest->append(Slice(ptr, length));
    if (s.is_ok()) {
      s = dest->flush();
    } else {
      assert(false);
    }
  } else {
    assert(false);
  }

  block_offset += HEADER_SIZE + length;
  return s;
}

} // namespace log
} // namespace mds
} // namespace morph
