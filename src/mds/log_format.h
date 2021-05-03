// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Code modified.

#ifndef MORPH_MDS_LOG_FORMAT_H
#define MORPH_MDS_LOG_FORMAT_H

namespace morph {
namespace mds {
namespace log {

using SequenceNumber = uint64_t;

enum ValueType {
  TYPE_DELETION = 0x0,          // This is delete operation
  TYPE_VALUE = 0x1              // This is put operation
};

enum RecordType {
  FULL_TYPE = 1,
  
  FIRST_TYPE = 2,
 
  MIDDLE_TYPE = 3,

  LAST_TYPE = 4
};

static const int MAX_RECORD_TYPE = LAST_TYPE;

static const int BLOCK_SIZE = 512;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int HEADER_SIZE = 4 + 2 + 1;

} // namespace log
} // namespace mds
} // namespace morph

#endif
