// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Code modified.

#include "common/nocopy.h"
#include "common/slice.h"
#include "common/env.h"
#include "log_format.h"

namespace morph {

class SequentialFile;

namespace mds {
namespace log {

class Reader: NoCopy {
 public:
  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this Reader is in use.
  //
  // If "checksum" is true, verify checksums if available.
  Reader(SequentialFile *file, bool checksum);

  ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  bool read_record(Slice *record, std::string *scratch);

 private:
  enum {
    FEOF = MAX_RECORD_TYPE + 1,

    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported
    BAD_RECORD = MAX_RECORD_TYPE + 2  
  };

  // Return type, or one of the preceding special values
  unsigned int read_physical_block(Slice *result);

  SequentialFile *const file;
  bool const checksum;
  char *const backing_store;
  Slice buffer;
  bool eof;

  // Offset of the last record returned by ReadRecord.
  uint64_t last_record_offset;
  // Offset of the first location past the end of buffer.
  uint64_t end_of_buffer_offset;
};

}

} // namespace mds

} // namespace morph