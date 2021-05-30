// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64                
//    count: fixed32               
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]
//
// Code modified. Note the current implementation does not need sequence number, it
// should used or to be removed in the future.

#include "write_batch.h"

#include "log_format.h"
#include "write_batch_internal.h"
#include "common/coding.h"
#include "common/utils.h"
#include "common/types.h"

namespace morph {

namespace mds {

static const size_t HEADER = 12;

WriteBatch::WriteBatch() {
  clear();
}

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::clear() {
  rep.clear();
  rep.resize(HEADER);
}

int WriteBatchInternal::count(const WriteBatch *b) {
  return decode_fixed_32(b->rep.data() + 8);
}

void WriteBatchInternal::set_count(WriteBatch *b, int n) {
  encode_fixed_32(&b->rep[8], n);
}

log::SequenceNumber WriteBatchInternal::sequence(const WriteBatch *b) {
  return log::SequenceNumber(decode_fixed_64(b->rep.data()));
}

void WriteBatchInternal::set_sequence(WriteBatch *b, log::SequenceNumber seq) {
  encode_fixed_64(&b->rep[0], seq);
}

void WriteBatchInternal::set_contents(WriteBatch *b, const Slice &contents) {
  assert(contents.size() >= HEADER);
  b->rep.assign(contents.data(), contents.size());
}

void WriteBatchInternal::append(WriteBatch *dst, const WriteBatch *src) {
  set_count(dst, count(dst) + count(src));
  assert(src->rep.size() >= HEADER);
  dst->rep.append(src->rep.data() + HEADER, src->rep.size() - HEADER);
}

void WriteBatch::put(const Slice &key, const Slice &value) {
  assert(key.size() > 0 && value.size() > 0);
  WriteBatchInternal::set_count(this, WriteBatchInternal::count(this) + 1);
  rep.push_back(static_cast<char>(log::TYPE_VALUE));
  put_length_prefixed_slice(&rep, key);
  put_length_prefixed_slice(&rep, value);
}

void WriteBatch::del(const Slice &key) {
  assert(key.size() > 0);
  WriteBatchInternal::set_count(this, WriteBatchInternal::count(this) + 1);
  rep.push_back(static_cast<char>(log::TYPE_DELETION));
  put_length_prefixed_slice(&rep, key);
}

Status WriteBatch::iterate(Handler *handler) const {
  Slice input(rep);
  if (input.size() < HEADER) {
    return Status::corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(HEADER);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    ++found;
    char tag = input[0];
    input.remove_prefix(1);

    switch(tag) {
      case log::TYPE_VALUE:
        if (get_length_prefixed_slice(&input, &key) &&
            get_length_prefixed_slice(&input, &value)) {
          handler->put(key, value);
        } else {
          return Status::corruption("bad WriteBatch put");
        }
        break;
      case log::TYPE_DELETION:
        if (get_length_prefixed_slice(&input, &key)) {
          handler->del(key);
        } else {
          return Status::corruption("bad WriteBatch delete");
        }
      default:
        return Status::corruption("unknown WriteBatch tag");
    }
  }

  if (found != WriteBatchInternal::count(this)) {
    return Status::corruption("WriteBatch has wrong count");
  } else {
    return Status::ok();
  }
}

size_t WriteBatch::approximate_size() const {
  return rep.size();
}


namespace {

class OpVectorInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence;
  OpVector *op;

  void put(const Slice& key, const Slice& value) override {
    op->emplace_back(0, key, value);
    sequence++;
  }

  void del(const Slice& key) override {
    op->emplace_back(1, key, Slice());
    sequence++;
  }
};
} // namespace

Status WriteBatchInternal::insert_into(const WriteBatch* batch, OpVector *op) {
  OpVectorInserter inserter;
  inserter.sequence = WriteBatchInternal::sequence(batch);
  inserter.op = op;
  return batch->iterate(&inserter);
}


} // namespace mds
} // namespace morph
