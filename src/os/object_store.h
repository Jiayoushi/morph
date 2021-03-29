#ifndef MORPH_OS_OBJECT_STORE
#define MORPH_OS_OBJECT_STORE

#include <array>
#include <memory>
#include <cstdint>
#include <cassert>
#include <unordered_map>
#include <os/buffer.h>
#include <os/block_store.h>
#include <os/kv_store.h>

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

  MSGPACK_DEFINE_ARRAY(start_lbn, end_lbn, start_pbn);

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
  using EXTENT_TREE_ITER = std::map<lbn_t, Extent>::iterator ;

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

  void record_request(const std::shared_ptr<IoRequest> req) {
    std::lock_guard<std::mutex> lock(pw_mutex);
    assert(pending_writes.find(req->buffers.front()->pbn) == pending_writes.end());
    pending_writes.emplace(std::make_pair(req->buffers.front()->pbn, req));
  }

  void remove_request(const std::shared_ptr<IoRequest> req) {
    std::lock_guard<std::mutex> lock(pw_mutex);
    pending_writes.erase(req->buffers.front()->pbn);
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
  std::unordered_map<pbn_t, std::shared_ptr<IoRequest>> pending_writes;

  // TODO: not used yet
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

  void stop();

 private:
  std::shared_ptr<Object> search_object(const std::string &name, bool create);

  std::shared_ptr<Object> allocate_object(const std::string &name);

  void object_write(std::shared_ptr<Object> object, const std::string &object_name, 
                    const uint32_t offset, const std::string &data);

  std::shared_ptr<Buffer> get_block(std::shared_ptr<Object> object, lbn_t lbn, lbn_t lbn_end, 
                                    bool create, uint32_t new_blocks);

  void write_buffer(const std::shared_ptr<Object> &objec, std::shared_ptr<Buffer> buffer, 
                    const char *data_ptr, uint32_t buf_offset, uint32_t size);

  std::shared_ptr<IoRequest> allocate_io_request(const std::shared_ptr<Object> owner, IoOperation op) {
    std::shared_ptr<IoRequest> req;

    req = std::make_shared<IoRequest>(op);
    if (op == OP_WRITE) {
      req->post_complete_callback = std::bind(&ObjectStore::post_write, this, owner, req);
    } else {
      req->post_complete_callback = std::bind(&ObjectStore::post_read, this, req);
    }

    return req;
  }

  // Called by the kv_store after a write call is journaled
  void post_log(std::shared_ptr<IoRequest> request) {
    //fprintf(stderr, "[os] post_log: let's fucking push request pbn(%d) op(%d)\n", 
    //  request->buffers.front()->pbn, request->op);
    block_store.push_request(request);
  }

  void post_write(const std::shared_ptr<Object> owner, const std::shared_ptr<IoRequest> &request) {
    // Important: remove the request should be placed before waking up any processes waiting for the buffers
    owner->remove_request(request);

    for (const std::shared_ptr<Buffer> &buffer: request->buffers) {
        //fprintf(stderr, "[OS] post_write: attempt to unmark %d dirty bit. request first pbn(%d)\n", 
        //  buffer->pbn, request->buffers.front()->pbn);
        flag_unmark(buffer, B_DIRTY);
        {
          std::unique_lock<std::mutex> lock(buffer->mutex);
          buffer->write_complete.notify_one();
        }
        //fprintf(stderr, "write release buffer %d ref %d\n", buffer->pbn, buffer->ref);
        buffer_manager.put_buffer(buffer);
    }

    //fprintf(stderr, "callback end\n");
  }

  void post_read(const std::shared_ptr<IoRequest> &request) {
    //fprintf(stderr, "post read called for %d\n", request->buffers.front()->pbn);
    for (const std::shared_ptr<Buffer> &buffer: request->buffers) {
      flag_mark(buffer, B_UPTODATE);
    }
  }

  void read_buffer(const std::shared_ptr<Object> &object, std::shared_ptr<Buffer> buffer);

  // Read the metadata and replay the data log after restart or crash
  void recover();

  // 0000000....00145-object_sara-138
  std::string get_data_key(uint32_t transaction_id, const std::string &object_name, uint32_t offset) {
    char buf[512];
    sprintf(buf, "%032d-%s-%u", transaction_id, object_name.c_str(), offset);
    return std::string(buf);
  }

  std::atomic<bool> running;

  //std::unique_ptr<std::thread> flush_thread;

  BlockStore block_store;

  BufferManager buffer_manager;

  KvStore kv_store;

  //BlockingQueue<std::shared_ptr<IoRequest>> write_queue;

  std::recursive_mutex index_mutex;

  std::unordered_map<std::string, std::shared_ptr<Object>> index;

  std::shared_ptr<spdlog::logger> logger;

  uint32_t id;

  // TODO: for testing only, need to be removed later.
  std::atomic<uint32_t> w_count;
  std::atomic<uint32_t> r_count;
};

}

#endif