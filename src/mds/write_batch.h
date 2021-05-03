// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.
//
// Code modified.

#ifndef MORPH_MDS_WRITE_BATCH_H
#define MORPH_MDS_WRITE_BATCH_H

#include <string>

#include "common/status.h"

namespace morph {

namespace mds {

class WriteBatch {
 public:
  class Handler {
   public:
    virtual ~Handler();
    virtual void put(const Slice &key, const Slice &value) = 0;
    virtual void del(const Slice &key) = 0;
  };

  WriteBatch();

  // Intentionally copyable.
  WriteBatch(const WriteBatch &) = default;
  WriteBatch & operator=(const WriteBatch &) = default;

  ~WriteBatch();

  void put(const Slice &key, const Slice &value);

  void del(const Slice &key);

  Status iterate(Handler *handler) const;

  void clear();
  
  // The size of the database changes caused by this batch
  size_t approximate_size() const;

 private:
  friend class WriteBatchInternal;

  std::string rep;
};

} // namespace mds

} // namespace morph

#endif