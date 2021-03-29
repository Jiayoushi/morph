#ifndef MORPH_OS_OBJECT_H
#define MORPH_OS_OBJECT_H

#include <array>
#include <memory>
#include <cstdint>
#include <cassert>
#include <unordered_map>
#include <os/buffer.h>
#include <os/block_store.h>

namespace morph {

const uint32_t INVALID_EXTENT = std::numeric_limits<uint32_t>::max();

struct Extent {
  uint32_t start;  // The first logical block number
  uint32_t end;    // The last logical block number (included)
  lbn_t lbn;       // The mapped first physical block number

  MSGPACK_DEFINE_ARRAY(start, end, lbn);

  Extent():
    start(INVALID_EXTENT)
  {}

  Extent(lbn_t lstart, lbn_t lend, lbn_t pstart):
    start(lstart),
    end(lend),
    lbn(pstart) {
    assert(lstart != INVALID_EXTENT);
  }

  bool valid() {
    return start != INVALID_EXTENT;
  }

  bool operator==(const Extent &rhs) const {
    return memcmp(this, &rhs, sizeof(Extent)) == 0;
  }
};


class Object {
 public:
  using EXTENT_TREE_ITER = std::map<lbn_t, Extent>::iterator ;

  Object():
    w_cnt(0),
    r_cnt(0),
    size(0)
  {}


  // On sucess, it returns true. ext is set to the extent that contains the lbn
  // On failure, it returns false.
  //   If there is a extent whose start is greater than lbn, then ext is set to that extent.
  //   If not, then ext is going to be set as a INVALID_EXTENT.
  bool search_extent(lbn_t lbn, Extent &ext);

  void insert_extent(lbn_t start, lbn_t end, lbn_t lbn) {
    extent_tree.emplace(end, Extent(start, end, lbn));
  }

  void record_request(const std::shared_ptr<IoRequest> req) {
    std::lock_guard<std::mutex> lock(pw_mutex);
    assert(pending_writes.find(req->buffers.front()->lbn) == pending_writes.end());
    pending_writes.emplace(std::make_pair(req->buffers.front()->lbn, req));
  }

  void remove_request(const std::shared_ptr<IoRequest> req) {
    std::lock_guard<std::mutex> lock(pw_mutex);
    pending_writes.erase(req->buffers.front()->lbn);
  }

  MSGPACK_DEFINE_ARRAY(extent_tree);

 private:
  friend class ObjectStore;

  std::atomic<uint32_t> w_cnt;
  std::atomic<uint32_t> r_cnt;

  // mutex for read and write operations
  std::mutex mutex;

  // mutex for the pending writes data structure below
  std::mutex pw_mutex;

  // a list of write requests to keep shared_ptr of write requests alive after aio is submitted
  // the key is the first physical block number of the buffers
  // TODO: we probably don't need this? As IoRequest is kept alive by the std::bind?
  std::unordered_map<lbn_t, std::shared_ptr<IoRequest>> pending_writes;

  // TODO: not used yet
  uint32_t size;

  // The metadata is entirely in memory, so we don't use direct extents here. Just a red black tree.
  // The tree is sorted by the the last logical block number covered by the extent
  std::map<lbn_t, Extent> extent_tree;
};

}

#endif