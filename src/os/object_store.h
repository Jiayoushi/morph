#ifndef MORPH_OS_OBJECT_STORE
#define MORPH_OS_OBJECT_STORE

#include <array>
#include <memory>
#include <cstdint>
#include <cassert>
#include <unordered_map>
#include <os/block_store.h>
#include <os/buffer.h>
#include <os/kv_store.h>
#include <os/object.h>

namespace morph {

// TODO:...
const uint8_t OBJECT_NOT_EXISTS = 1;
const uint8_t NO_CONTENT = 2;          // 1. read parts that the object has not yet written
const uint8_t OBJECT_NAME_INVALID = 3;


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

  std::shared_ptr<IoRequest> allocate_io_request(const std::shared_ptr<Object> owner, IoOperation op);

  // Called by the kv_store after a write call is journaled
  void post_log(std::shared_ptr<IoRequest> request) {
    //fprintf(stderr, "[os] post_log: let's fucking push request lbn(%d) op(%d)\n", 
    //  request->buffers.front()->lbn, request->op);
    block_store.push_request(request);
  }

  void post_write(const std::shared_ptr<Object> owner, const std::shared_ptr<IoRequest> &request);

  void post_read(const std::shared_ptr<IoRequest> &request) {
    //fprintf(stderr, "post read called for %d\n", request->buffers.front()->lbn);
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

  BlockStore block_store;

  BufferManager buffer_manager;

  KvStore kv_store;

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