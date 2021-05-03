// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef MORPH_MDS_LOG_WRITER_H
#define MORPH_MDS_LOG_WRITER_H

#include "common/nocopy.h"
#include "common/status.h"
#include "mds/log_format.h"
#include "common/env.h"

namespace morph {

class WritableFile;

namespace mds {
namespace log {

class Writer: NoCopy {
 public:
  explicit Writer(WritableFile *dest);

  ~Writer();

  Status add_record(const Slice &slice);

 private:
  Status emit_physical_record(RecordType type, const char *ptr, size_t length);

  // Current offset in block
  uint32_t block_offset;

  WritableFile *dest;

  uint32_t type_crc[MAX_RECORD_TYPE + 1];
};

} // namespace log
} // namespace mds
} // namespace morph



#endif
