#ifndef MORPH_OS_OBJECT_STORE
#define MORPH_OS_OBJECT_STORE

#include <array>
#include <memory>
#include <cstdint>
#include <unordered_map>
#include <os/buffer.h>
#include <os/block_store.h>
#include <os/mdstore.h>

namespace morph {


struct Extent {
  lbn_t start_lbn;       // The first logical block number
  lbn_t end_lbn;        // The last logical block number (included)
  sector_t start_pbn;    // The mapped first physical block number

  Extent() 
  {}

  Extent(lbn_t lstart, lbn_t lend, sector_t pstart):
    start_lbn(lstart),
    end_lbn(lend),
    start_pbn(pstart) {

  }

  bool operator==(const Extent &rhs) {
    return memcmp(this, &rhs, sizeof(Extent)) == 0;
  }
};


class Object {
 public:
  typedef std::map<lbn_t, Extent>::iterator EXTENT_TREE_ITER;

  Object()
  {}

  //    [        ]
  //  [  ] [ ] [   ]
  std::vector<Extent> search_extent(lbn_t target_lbn_start, lbn_t target_lbn_end) {
    std::vector<Extent> res;
    EXTENT_TREE_ITER iter;
    lbn_t start;
    lbn_t end;
    
    for (iter = extent_tree.lower_bound(target_lbn_start);
         iter != extent_tree.end();
         ++iter) {
      start = iter->second.start_lbn;
      end = iter->second.end_lbn;

      if (end < target_lbn_start || start > target_lbn_end) {
        break;
      }

      res.push_back(iter->second);
    }

    return res;
  }

  void insert_extent(lbn_t start_lbn, lbn_t end_lbn, sector_t start_pbn) {
    assert(search_extent(start_lbn, end_lbn).empty());

    extent_tree.emplace(end_lbn, Extent(start_lbn, end_lbn, start_pbn));
  }

 private:
  // The metadata is entirely in memory, so we don't use direct extents here. Just a red black tree.
  // The tree is sorted by the last logical number covered by the extent
  std::map<lbn_t, Extent> extent_tree;
};

class ObjectStore {
 public:
  void PutObject(const std::string &object_name, const uint32_t offset, const std::string &body);

 private:
  void object_write_data(std::shared_ptr<Object> object, const uint32_t offset, const std::string &data);

  BlockStore block_store;

  BufferManager buffer_manager;

  MdStore md_store;

  std::unordered_map<std::string, std::shared_ptr<Object>> objects;
};

}

#endif