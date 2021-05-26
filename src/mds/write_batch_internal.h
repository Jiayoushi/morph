// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors
//
// Code modified.

#ifndef MORPH_MDS_WRITE_BATCH_INTERNAL_H
#define MORPH_MDS_WRITE_BATCH_INTERNAL_H

#include "log_format.h"
#include "write_batch.h"
#include "common/types.h"

namespace morph {

namespace mds {

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface
class WriteBatchInternal {
 public:
  // Return the number of entries
  static int count(const WriteBatch *batch);

  // Set the number of entries
  static void set_count(WriteBatch *batch, int n);

  // Return the sequence number for the start of this batch
  static log::SequenceNumber sequence(const WriteBatch *batch);

  static void set_sequence(WriteBatch *batch, log::SequenceNumber seq);

  static Slice contents(const WriteBatch *batch) {
    return Slice(batch->rep);
  }

  static size_t byte_size(const WriteBatch *batch) {
    return batch->rep.size();
  }

  static void set_contents(WriteBatch *batch, const Slice &contents);

  static Status insert_into(const WriteBatch* batch, OpVector *op);

  static void append(WriteBatch *dst, const WriteBatch *src);
};

} // namespace mds

} // namespace morph


#endif