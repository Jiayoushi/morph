#ifndef MORPH_OS_OBJECT_STORE
#define MORPH_OS_OBJECT_STORE

#include <array>
#include <memory>
#include <cstdint>
#include <cassert>
#include <unordered_map>

#include "block_store.h"
#include "buffer.h"
#include "kv_store.h"
#include "object.h"
#include "block_allocator.h"

namespace morph {

namespace os {

class ObjectStore {
 public:
  ObjectStore() = delete;

  explicit ObjectStore(const std::string &name, 
              ObjectStoreOptions oso = ObjectStoreOptions());

  ~ObjectStore();

  int put_object(const std::string &object_name, const uint32_t offset, 
                 const std::string &body, 
                 std::function<void(void)> on_apply=nullptr);

  int get_object(const std::string &object_name, std::string *buf, 
                 const uint32_t offset, const uint32_t size);

  int put_metadata(const std::string &object_name, 
                   const std::string &attribute, const std::string &value);

  int get_metadata(const std::string &object_name,
                   const std::string &attribute, std::string *buf);

  int delete_metadata(const std::string &object_name,
                      const std::string &attribute);

  const ObjectStoreOptions opts;

  void stop();

 private:
  std::shared_ptr<Object> search_object(const std::string &name, bool create);

  std::shared_ptr<Object> allocate_object(const std::string &name);

  void object_write(std::shared_ptr<Object> object, 
                    const std::string &object_name, const uint32_t offset, 
                    const std::string &data, 
                    std::function<void(void)> on_apply);

  void object_large_write(std::shared_ptr<Object> object, 
                          const std::string &object_name, 
                          const uint32_t offset, const std::string &data,
                          std::function<void(void)> on_apply);

    void object_small_write(std::shared_ptr<Object> object, 
                            const std::string &object_name, 
                            const uint32_t offset, 
                            const std::string &data,
                            std::function<void(void)> on_apply);

    Buffer * get_block(std::shared_ptr<Object> object, lbn_t lbn,
                      LogHandle *log_handle=nullptr,
                      lbn_t lbn_end=0, bool create=false, 
                      uint32_t new_blocks=0);

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
                        Buffer *buffer, const char *data_ptr, 
                        uint32_t buf_offset, uint32_t size, 
                        bool set_flags = true);


  /*
   * Copies the data from block located at "file_off" to "new_buffer".
   */
  size_t cow_write_buffer(const std::shared_ptr<Object> &object,
                          const uint32_t file_off, Buffer *new_buffer,
                          uint32_t buf_off, size_t write_size, 
                          const char *data_ptr);

  void log_write(const std::shared_ptr<Object> &object, 
                 const std::string &object_name, IoRequest *request, 
                 std::string data, uint32_t offset, bool bitmap_modified);

  void persist_metadata();

  // Called by the kv_store after a write call is journaled
  void post_log(IoRequest *request) {
    for (Buffer *buffer: request->buffers) {
      buffer->mutex.lock();
    }
    block_store.submit_request(request);
  }

  void after_write(const std::shared_ptr<Object> &object, 
                   IoRequest *request, bool finished);

  void after_read(IoRequest *request, bool finished);

  void read_buffer_from_disk(const std::shared_ptr<Object> &object, 
                             Buffer *buffer);

  // Read the metadata and replay the data log after restart or crash
  void recover();

  uint64_t assign_request_id() {
    return request_id++;
  }

  // 0000000....00145-object_sara-138
  std::string get_data_key(uint32_t transaction_id, 
                           const std::string &object_name, 
                           uint32_t offset) {
    char buf[512];
    sprintf(buf, "%032d-%s-%u", transaction_id, object_name.c_str(), offset);
    return std::string(buf);
  }

  std::string get_object_metadata_key(const std::string &object_name,
                                      const std::string &attribute) {
    return object_name + "-" + attribute;
  }

  bool is_block_aligned(uint32_t addr) const {
    return addr % opts.bso.block_size == 0;
  }

  bool has_pending_operations() const {
    return unfinished_writes > 0 || unfinished_reads > 0;
  }

  IoRequest *allocate_request(IoOperation operation) {
    IoRequest *request = new IoRequest(assign_request_id(), operation);
    
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
  }

  std::string name;

  std::atomic<bool> running;

  BlockStore block_store;

  BufferManager buffer_manager;

  KvStore kv_store;

  BlockAllocator block_allocator;

  std::recursive_mutex index_mutex;

  std::unordered_map<std::string, std::shared_ptr<Object>> index;

  std::shared_ptr<spdlog::logger> logger;

  std::atomic<uint32_t> unfinished_writes;

  std::atomic<uint32_t> unfinished_reads;

  uint64_t request_id;
};

} // namespace os

} // namespace morph

#endif
