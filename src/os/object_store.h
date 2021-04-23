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

  int put_object(const std::string &object_name, const uint32_t offset, 
    const std::string &body);

  int get_object(const std::string &object_name, std::string &buf, 
    const uint32_t offset, const uint32_t size);

  const ObjectStoreOptions opts;

  void stop();

 private:
  std::shared_ptr<Object> search_object(const std::string &name, bool create);

  std::shared_ptr<Object> allocate_object(const std::string &name);

  void object_write(std::shared_ptr<Object> object, 
    const std::string &object_name, const uint32_t offset, 
    const std::string &data);

  void object_large_write(std::shared_ptr<Object> object, 
    const std::string &object_name, const uint32_t offset, 
    const std::string &data);

  void object_small_write(std::shared_ptr<Object> object, 
    const std::string &object_name, const uint32_t offset, 
    const std::string &data);

  Buffer * get_block(std::shared_ptr<Object> object, lbn_t lbn, 
    lbn_t lbn_end = 0, bool create = false, uint32_t new_blocks = 0);

  /*
   * Copies the data in "data_ptr" to buffer.
   * 
   * params:
   *   buf_offset: the offset of the buffer to be copied. Note it's not the start of data_ptr.
   *   size: the number of bytes to copy.
   * 
   * return: 
   *   how many bytes are copied to the buffer.
   */
  uint32_t write_buffer(const std::shared_ptr<Object> &object, 
      Buffer * buffer, const char *data_ptr, 
      uint32_t buf_offset, uint32_t size, bool set_flags = true);


  /*
   * Copies the data from block located at "file_off" to "new_buffer".
   */
  size_t cow_write_buffer(const std::shared_ptr<Object> &object,
      const off_t file_off, Buffer *new_buffer,
      off_t buf_off, size_t write_size, const char *data_ptr);

  void log_write(const std::shared_ptr<Object> &object, 
    const std::string &object_name, IoRequest *request, 
    std::string data, uint32_t offset, bool bitmap_modified);

  void persist_metadata();

  // Called by the kv_store after a write call is journaled
  void post_log(IoRequest *request) {
    //fprintf(stderr, "[os] submitting write request(%lu)\n", 
    //  request->get_id());
    block_store.submit_request(request);
  }

  void after_write(const std::shared_ptr<Object> &object, 
    IoRequest *request, bool finished);

  void after_read(IoRequest *request, bool finished);

  void read_buffer(const std::shared_ptr<Object> &object, 
    Buffer *buffer);

  // Read the metadata and replay the data log after restart or crash
  void recover();

  uint64_t assign_request_id() {
    return request_id++;
  }

  // 0000000....00145-object_sara-138
  std::string get_data_key(uint32_t transaction_id, const std::string &object_name, 
      uint32_t offset) {
    char buf[512];
    sprintf(buf, "%032d-%s-%u", transaction_id, object_name.c_str(), offset);
    return std::string(buf);
  }

  bool is_block_aligned(uint32_t addr) const {
    return addr % opts.bso.block_size == 0;
  }

  bool has_pending_operations() const {
    //fprintf(stderr, "unfished_writes %u reads %u\n",
    //  unfinished_writes.load(), unfinished_reads.load());
    return unfinished_writes > 0 || unfinished_reads > 0;
  }

  IoRequest *start_request(IoOperation operation) {
    IoRequest *request = new IoRequest(assign_request_id(), operation);
    
    assert(operation == OP_READ || operation == OP_WRITE);

    if (operation == OP_READ) {
      ++unfinished_reads;
    } else {
      ++unfinished_writes;
    }

    return request;
  }

  void end_request(IoRequest *request) {
    if (request->op == OP_READ) {
      --unfinished_reads;
    } else {
      --unfinished_writes;
    }
    delete request;
  }

  std::atomic<bool> running;

  BlockStore block_store;

  BufferManager buffer_manager;

  KvStore kv_store;

  std::recursive_mutex index_mutex;

  std::unordered_map<std::string, std::shared_ptr<Object>> index;

  std::shared_ptr<spdlog::logger> logger;

  uint32_t id;

  std::atomic<uint32_t> unfinished_writes;

  std::atomic<uint32_t> unfinished_reads;

  uint64_t request_id;
};

}

#endif
