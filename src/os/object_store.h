#ifndef MORPH_OS_OBJECT_STORE
#define MORPH_OS_OBJECT_STORE

#include <array>
#include <memory>
#include <cstdint>
#include <cassert>
#include <unordered_map>
#include <os/buffer.h>
#include <os/block_store.h>
#include <os/mdstore.h>

namespace morph {

// TODO:...
const uint8_t OBJECT_NOT_EXISTS = 1;
const uint8_t NO_CONTENT = 2;          // 1. read parts that the object has not yet written
const uint8_t OBJECT_NAME_INVALID = 3;

const uint32_t INVALID_EXTENT = std::numeric_limits<uint32_t>::max();

struct Extent {
  lbn_t start_lbn;       // The first logical block number
  lbn_t end_lbn;         // The last logical block number (included)
  pbn_t start_pbn;       // The mapped first physical block number

  Extent():
    start_lbn(INVALID_EXTENT)
  {}

  Extent(lbn_t lstart, lbn_t lend, pbn_t pstart):
    start_lbn(lstart),
    end_lbn(lend),
    start_pbn(pstart) {
    assert(lstart != INVALID_EXTENT);
  }

  bool valid() {
    return start_lbn != INVALID_EXTENT;
  }

  bool operator==(const Extent &rhs) const {
    return memcmp(this, &rhs, sizeof(Extent)) == 0;
  }

};


class Object {
 public:
  typedef std::map<lbn_t, Extent>::iterator EXTENT_TREE_ITER;

  Object():
    w_cnt(0),
    r_cnt(0),
    size(0)
  {}


  // On sucess, it returns true. ext is set to the extent that contains the lbn
  // On failure, it returns false.
  //   If there is a extent whose start_lbn is greater than lbn, then ext is set to that extent.
  //   If not, then ext is going to be set as a INVALID_EXTENT.
  bool search_extent(lbn_t lbn, Extent &ext) {
    std::vector<Extent> res;
    EXTENT_TREE_ITER iter;
    lbn_t start;
    lbn_t end;
    
    iter = extent_tree.lower_bound(lbn);

    if (iter == extent_tree.end()) {
      ext.start_lbn = INVALID_EXTENT;
      return false;
    }

    ext = iter->second;
    return lbn >= iter->second.start_lbn && lbn <= iter->second.end_lbn;
  }

  void insert_extent(lbn_t start_lbn, lbn_t end_lbn, pbn_t start_pbn) {
    extent_tree.emplace(end_lbn, Extent(start_lbn, end_lbn, start_pbn));
  }

 private:
  friend class ObjectStore;

  std::atomic<uint32_t> w_cnt;
  std::atomic<uint32_t> r_cnt;

  std::mutex mutex;

  std::list<std::shared_ptr<Buffer>> dirty_buffers;

  //std::bitset<64> flag;

  uint32_t size;

  // The metadata is entirely in memory, so we don't use direct extents here. Just a red black tree.
  // The tree is sorted by the the last logical block number covered by the extent
  std::map<lbn_t, Extent> extent_tree;
};


class ObjectStore {
 public:
  ObjectStore() = delete;

  ObjectStore(uint32_t id, ObjectStoreOptions oso = ObjectStoreOptions());

  ~ObjectStore();

  int put_object(const std::string &object_name, const uint32_t offset, const std::string &body);

  int get_object(const std::string &object_name, std::string &buf, const uint32_t offset, const uint32_t size);

  const ObjectStoreOptions opts;

 private:
  std::shared_ptr<Object> search_object(const std::string &name, bool create);

  void object_write(std::shared_ptr<Object> object, const uint32_t offset, const std::string &data);

  std::shared_ptr<Buffer> get_block(std::shared_ptr<Object> object, lbn_t lbn, lbn_t lbn_end, 
                                    bool create, uint32_t new_blocks);

  void write_buffer(std::shared_ptr<Object> object, std::shared_ptr<Buffer> buffer, 
                    const char *data_ptr, uint32_t buf_offset, uint32_t size);

  void read_buffer(std::shared_ptr<Object> object, std::shared_ptr<Buffer> buffer);

  void flush();

  std::atomic<bool> running;

  std::unique_ptr<std::thread> flush_thread;

  BlockStore block_store;

  BufferManager buffer_manager;

  //MdStore md_store;

  BlockingQueue<std::shared_ptr<Object>> dirty_objects;

  std::mutex index_mutex;

  std::unordered_map<std::string, std::shared_ptr<Object>> index;

  std::shared_ptr<spdlog::logger> logger;

  uint32_t id;

  std::atomic<uint32_t> w_count;
  std::atomic<uint32_t> r_count;
};

}

#endif