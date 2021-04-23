#ifndef MORPH_OS_OBJECT_H
#define MORPH_OS_OBJECT_H

#include <array>
#include <memory>
#include <cstdint>
#include <cassert>
#include <unordered_map>
#include <os/buffer.h>
#include <os/block_store.h>
#include <rpc/msgpack.hpp>

namespace morph {

const uint32_t INVALID_EXTENT = std::numeric_limits<uint32_t>::max();

struct Extent {
  /*
   * The offset of the starting block.
   * It can be 7th block, or 155th block, ... 
   * Note it's always block aligned.
   */  
  off_t off_start;

  off_t off_end;    // The offset of the ending block

  lbn_t lbn;             // The mapped first logical block number

  MSGPACK_DEFINE_ARRAY(off_start, off_end, lbn);

  Extent():
    off_start(INVALID_EXTENT)
  {}

  Extent(off_t s_off, off_t e_off, lbn_t logical_start):
    off_start(s_off),
    off_end(e_off),
    lbn(logical_start) {
    assert(s_off != INVALID_EXTENT);
  }

  bool valid() {
    return off_start != INVALID_EXTENT;
  }

  bool operator==(const Extent &rhs) const {
    return memcmp(this, &rhs, sizeof(Extent)) == 0;
  }

  lbn_t compute_lbn(off_t off) {
    assert(off >= off_start && off <= off_end);
    return lbn + off - off_start;
  }
};


class Object {
 public:
  using EXTENT_TREE_ITER = std::map<lbn_t, Extent>::iterator;

  Object() = delete;

  Object(const std::string &name):
    name(name),
    size(0)
  {}


  // TODO: the interface is really garbage. really need to change it.
  // On sucess, it returns true. ext is set to the extent that contains the lbn
  // On failure, it returns false.
  //   If there is a extent whose start is greater than lbn, then ext is set to that extent.
  //   If not, then ext is going to be set as a INVALID_EXTENT.
  bool search_extent(off_t off, Extent *ext);

  // Get the pointer to the extent that covers the logical block "lbn".
  //
  // Return:
  //   On success, returns the pointer.
  //   On failure, returns nullptr.
  Extent *get_extent(off_t off) {
    auto iter = extent_tree.lower_bound(off);
    if (iter != extent_tree.end()) {
      return &iter->second;
    } else {
      return nullptr;
    }
  }

  void insert_extent(off_t start, off_t end, lbn_t lbn) {
    extent_tree.emplace(end, Extent(start, end, lbn));
  }

  std::vector<std::pair<lbn_t, uint32_t>> delete_extent_range(off_t start, 
      off_t end);

  std::string get_name() const {
    return name;
  }

  MSGPACK_DEFINE_ARRAY(name, extent_tree);

 private:
  friend class ObjectStore;

  std::string name;

  // Ensure thread-safty of 
  //   read
  //   write
  std::mutex mutex;

  // TODO: not used yet
  uint32_t size;

  // Sorted by the the last logical block offset covered by the extent
  std::map<off_t, Extent> extent_tree;
};

}

#endif
