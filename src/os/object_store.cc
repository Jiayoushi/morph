#include <os/object_store.h>

#include <cassert>
#include <spdlog/fmt/bundled/printf.h>

#include "common/utils.h"
#include "common/filename.h"
#include "common/logger.h"
#include "error_code.h"

namespace morph {

namespace os {

ObjectStore::ObjectStore(const std::string &name, ObjectStoreOptions oso):
    name(name),
    opts(oso),
    block_store(name, opts.bso),
    kv_store(name, opts.kso),
    block_allocator(opts.bao),
    buffer_manager(oso.bmo),
    unfinished_writes(0),
    unfinished_reads(0),
    running(true),
    request_id(0) {


  logger = init_logger(name);
  assert(logger != nullptr);

  logger->debug("logger initialized");

  if (opts.recover) {
    recover();
  }
}

ObjectStore::~ObjectStore() {
  if (running) {
    stop();
  }
}

void ObjectStore::recover() {
  using rocksdb::ReadOptions;
  using rocksdb::Status;
  using rocksdb::Slice;
  
  std::string bitmap;
  Status s;
  std::shared_ptr<Object> object;
  std::string object_name;

  // Restore bitmap
  auto bitmap_iter = kv_store.db->NewIterator(
                         ReadOptions(),
                         kv_store.get_cf_handle(CF_INDEX::CF_SYS_BITMAP));
  for (bitmap_iter->SeekToFirst();
       bitmap_iter->Valid();
       bitmap_iter->Next()) {
    block_allocator.restore_metadata(bitmap_iter->key().ToString(),
                                     bitmap_iter->value().ToString());
  }
  delete bitmap_iter;


  // Restore object metadata
  auto obj_meta_iter = kv_store.db->NewIterator(
                             ReadOptions(), 
                             kv_store.get_cf_handle(CF_INDEX::CF_OBJ_META));

  for (obj_meta_iter->SeekToFirst(); 
       obj_meta_iter->Valid(); 
       obj_meta_iter->Next()) {
    object_name = obj_meta_iter->key().ToString();

    object = search_object(object_name, false);
    assert(object == nullptr);

    object = allocate_object(object_name);

    assert(obj_meta_iter->value().size() > 0);
    deserialize(obj_meta_iter->value().ToString(), *object);
  }
  delete obj_meta_iter;
}

void ObjectStore::replay_data_logs() {
  using rocksdb::ReadOptions;
  using rocksdb::Status;
  using rocksdb::Slice;

  // The request type is set to small write so it can be deleted afterwards
  IoRequest *request = nullptr;
 
  uint32_t prev_txn_id = 0;
  auto data_iter = kv_store.db->NewIterator(
                                ReadOptions(),
                                kv_store.get_cf_handle(CF_INDEX::CF_OBJ_DATA));
  for (data_iter->SeekToFirst();
       data_iter->Valid();
       data_iter->Next()) {
    std::string key = data_iter->key().ToString();    
    std::string value = data_iter->value().ToString();
    uint32_t txn_id;
    lbn_t lbn;

    // TODO: limit the number of buffers under the max events block store can handle.
    //       This should be handled by block store. Need to be refactored later.
    if (request && request->buffers.size() > 20) {
      submit_request(request);

      // TODO: this is only necessary for the last one
      //       or you can just use a sync call to the block store.
      //       Either way, change this later.
      request->wait_for_complete();
      request = nullptr;
    }

    if (request == nullptr) {
      request = allocate_request(OP_SMALL_WRITE);
      request->after_complete_callback = [request, this]() {
        after_write(request, false);
      };
    }

    parse_data_key(key, &txn_id, &lbn);
    assert(txn_id);

    Buffer *buffer = buffer_manager.get_buffer(lbn);
    request->buffers.push_back(buffer);
  }

  delete data_iter;
}

std::shared_ptr<Object> ObjectStore::allocate_object(const std::string &name) {
  std::shared_ptr<Object> object;

  std::lock_guard<std::recursive_mutex> lock(index_mutex);
 
  object = std::make_shared<Object>(name);
  index.emplace(name, object);

  return object;
}

std::shared_ptr<Object> ObjectStore::search_object(const std::string &name, 
    bool create) {
  std::lock_guard<std::recursive_mutex> lock(index_mutex);

  auto f = index.find(name);

  if (f == index.end()) {
    if (!create) {
      return nullptr;
    }

    return allocate_object(name);
  } else {
    return f->second;
  }
}


int ObjectStore::put_object(const std::string &object_name, 
                            const uint32_t offset, 
                            const std::string &data, 
                            std::function<void(void)> on_apply) {
  std::shared_ptr<Object> object;

  if (object_name.empty()) {
    return os::OBJECT_NAME_INVALID;
  }

  object = search_object(object_name, true);

  logger->debug(fmt::sprintf("[ObjectStore] put_object object_name(%s) offset(%d) size(%d)\n",
    object_name.c_str(), offset, data.size()));

  if (!data.empty()) {
    object_write(object, object_name, offset, data, on_apply);
  }

  logger->debug(fmt::sprintf("[ObjectStore] put_object: object_name(%s) offset(%d): success\n",
    object_name.c_str(), offset));

  return 0;
}


// TODO: it should be deleted??
// rmw part
// cow part
// in the same io request
// first wait for the rmw is logged
// then after all data are written, time to change and log the metadata
// after the metadata are logged, 
void ObjectStore::object_write(std::shared_ptr<Object> object, 
                               const std::string &object_name, 
                               const uint32_t offset, 
                               const std::string &data, 
                               std::function<void(void)> on_apply) {
  std::lock_guard<std::mutex> lock(object->mutex);

  if (data.size() >= opts.cow_data_size) {
    object_large_write(object, object_name, offset, data, on_apply);
  } else {
    object_small_write(object, object_name, offset, data, on_apply);
  }
}

void ObjectStore::object_large_write(std::shared_ptr<Object> object, 
                                     const std::string &object_name, 
                                     const uint32_t offset, 
                                     const std::string &data, 
                                     std::function<void(void)> on_apply) {
  using NewBufferList = typename std::list<Buffer *>;

  const uint32_t start_off = offset / opts.bso.block_size,
                 end_off = (offset + data.size()) / opts.bso.block_size;
  const uint32_t total_blocks = end_off - start_off + 1;
  Buffer *buffer;
  IoRequest *request = allocate_request(OP_LARGE_WRITE);
  std::list<Buffer *>::iterator iter;
  const char *write_ptr = data.c_str();
  uint32_t write_size;
  lbn_t new_start_blk;
  std::shared_ptr<LogHandle> handle;

  assert(total_blocks >= 3);

  handle = kv_store.start_transaction();

  // Allocate blocks and get buffers
  new_start_blk = block_allocator.allocate_blocks(total_blocks, handle.get());

  for (uint32_t blk = new_start_blk; 
      blk < new_start_blk + total_blocks; 
      ++blk) {
    buffer = buffer_manager.get_buffer(blk);
    flag_mark(buffer, B_NEW);
    request->buffers.push_back(buffer);
  }
  iter = request->buffers.begin();

  // process head block
  write_size = opts.bso.block_size - (offset % opts.bso.block_size);
  write_ptr += cow_write_buffer(object, offset, *iter,
        offset % opts.bso.block_size, write_size, write_ptr);

  // process middle blocks
  write_size = opts.bso.block_size;
  for (uint32_t blk = new_start_blk + 1; 
      blk < new_start_blk + total_blocks - 1; 
      ++blk) {
    ++iter;
    write_ptr += write_buffer(object, *iter, write_ptr, 0, write_size);
  }

  // process tail block
  ++iter;
  write_size = data.size() - (write_ptr - data.c_str());
  write_ptr += cow_write_buffer(object, new_start_blk + total_blocks - 1, 
    *iter, 0, write_size, write_ptr);

  assert(*write_ptr == '\0');
  assert((++iter) == request->buffers.end());


  // TODO: it's better to let the object's insert_extent/remove_extent to
  //       handle the log operation. Probably? Since the object's 
  //       index tree might become complex
  auto blocks_to_free = object->delete_extent_range(start_off, end_off);
  for (const auto &pair: blocks_to_free) {
    block_allocator.deallocate_blocks(pair.first, pair.second, handle.get());
  }
  object->insert_extent(start_off, end_off, new_start_blk);
  std::string object_meta = std::move(serialize(*object));
  handle->put(CF_INDEX::CF_OBJ_META,
              object->name,
              std::move(object_meta));

  handle->post_log_callback = [this, request, on_apply]() {
    if (on_apply) {
      on_apply();
    }
    end_request(request);
    delete request;
  };

  request->after_complete_callback = 
      [this, object, request, handle]() mutable {
    this->after_write(request, false);
    kv_store.end_transaction(handle);
  };

  block_store.submit_request(request);

  logger->debug(fmt::sprintf("offset(%d) size(%d) large write exit!\n", 
    offset, data.size()));
}

void ObjectStore::object_small_write(std::shared_ptr<Object> object, 
                                     const std::string &object_name, 
                                     const uint32_t offset, 
                                     const std::string &data,
                                     std::function<void(void)> on_apply) {
  const lbn_t lbn_start = offset / opts.bso.block_size;
  const lbn_t lbn_end = (offset + data.size()) / opts.bso.block_size;
  Buffer * buffer;
  bool obj_meta_changed = false;
  uint32_t unwritten = data.size();
  uint32_t write_len = 0, buf_off = 0, new_blocks = 0;
  const char *data_ptr = data.c_str();
  IoRequest *request = allocate_request(OP_SMALL_WRITE);
  std::shared_ptr<LogHandle> log_handle;

  log_handle = kv_store.start_transaction();

  log_handle->post_log_callback = std::bind(&ObjectStore::submit_request, this, 
                                            request);

  request->after_complete_callback = 
    [this, object, request, on_apply]() {
      if (on_apply) {
        on_apply();
      }
      after_write(request, true);
    };

  for (lbn_t lbn = lbn_start; 
       lbn <= lbn_end; 
       ++lbn, unwritten -= write_len, data_ptr += write_len) {
    buf_off = lbn == lbn_start ? offset % opts.bso.block_size: 0;
    write_len = std::min(opts.bso.block_size - buf_off, unwritten);

    buffer = get_block(object, lbn, log_handle.get(), 
                       lbn_end, true, new_blocks);

    // When get_block tries to allocate, it will allocate more than one blocks
    // So all blocks following the first one should be marked B_NEW
    // We use this new_blocks to count how many blocks following the first one
    // should be marked as new_block
    if (flag_marked(buffer, B_NEW)) {
      obj_meta_changed = true;
      new_blocks = new_blocks == 0 ? lbn_end - lbn : new_blocks - 1;
    }
    
    // TODO(required): if the buffer is comitting, need to copy the buffer

    write_buffer(object, buffer, data_ptr, buf_off, write_len);
    log_handle->put(CF_INDEX::CF_OBJ_DATA,
                    get_data_key(log_handle->transaction->id, lbn),
                    std::move(std::string(data_ptr, write_len)));

    request->buffers.push_back(buffer);
  }

  assert(!request->buffers.empty());

  if (obj_meta_changed) {
    std::string v = serialize(*object);
    assert(v.size() > 0);
    log_handle->put(CF_INDEX::CF_OBJ_META, 
                    object_name, 
                    std::move(v));
  }

  kv_store.end_transaction(log_handle);
}

Buffer * ObjectStore::get_block(std::shared_ptr<Object> object, 
                                uint32_t target, LogHandle *handle,
                                uint32_t target_end, bool create, 
                                uint32_t new_blocks) {
  Buffer *buffer;
  Extent extent;
  uint32_t count;    /* How many blocks to allcoate if ncessary */
  lbn_t lbn;
  bool allocated = false;

  if (object->search_extent(target, &extent)) {
    lbn = extent.lbn + (target - extent.off_start);
  } else {
    if (!create) {
      return nullptr;
    }

    if (extent.valid()) {
      count = std::min(target_end - target + 1,
        extent.off_start - target);
    } else {
      count = target_end - target + 1;
    }

    lbn = block_allocator.allocate_blocks(count, handle);

    object->insert_extent(target, target + count - 1, lbn);
    allocated = true;
  }

  buffer = buffer_manager.get_buffer(lbn);

  if (allocated || new_blocks > 0) {
    flag_mark(buffer, B_NEW);
  }

  return buffer;
}

uint32_t ObjectStore::write_buffer(const std::shared_ptr<Object> &object, 
                                   Buffer *buffer, const char *data_ptr, 
                                   uint32_t buf_offset,
                                   uint32_t size, bool update_flags) {
  if (!is_block_aligned(size) && !flag_marked(buffer, B_NEW)) {
    read_buffer_from_disk(object, buffer);
  }

  buffer->copy_in(data_ptr, 0, buf_offset, size);

  if (update_flags) {
    flag_mark(buffer, B_DIRTY);
    flag_mark(buffer, B_UPTODATE);
    flag_unmark(buffer, B_NEW);
  }

  return size;
}

size_t ObjectStore::cow_write_buffer(const std::shared_ptr<Object> &object,
                                     const uint32_t file_off, 
                                     Buffer *dst_buffer,
                                     uint32_t buf_off, size_t write_size, 
                                     const char *data_ptr) {
  Buffer *src_buffer;

  // If it's a partial write, we need to read the old block first (if any).
  if (!is_block_aligned(write_size)) {
    src_buffer = get_block(object, file_off);

    if (src_buffer != nullptr) {
      if (!flag_marked(src_buffer, B_UPTODATE)) {
        read_buffer_from_disk(object, src_buffer);
      }

      write_buffer(object, dst_buffer, src_buffer->buf,
          0, opts.bso.block_size, false);

      buffer_manager.put_buffer(src_buffer);
    }
  }

  write_buffer(object, dst_buffer, data_ptr,
      buf_off, write_size);

  return write_size;
}

// TODO: is it a waste to allocate a request for a single buffer?
//       should get all the buffers ready before anything else!
void ObjectStore::read_buffer_from_disk(const std::shared_ptr<Object> &object, 
                                        Buffer *buffer) {
  IoRequest *request;

  if (flag_marked(buffer, B_UPTODATE)) {
    return;
  }

  request = allocate_request(OP_READ);
  request->after_complete_callback = std::bind(&ObjectStore::after_read, 
    this, request, false);
  request->buffers.push_back(buffer);

  block_store.submit_request(request);

  request->wait_for_complete();
  end_request(request);
}

// TODO: refactor some portion of the code into read_object
// TODO: should allow some parallelism. don't hold the lock for the 
//       entire read operation
int ObjectStore::get_object(const std::string &object_name, std::string *out,
                            const uint32_t offset, const uint32_t size) {
  const lbn_t lbn_start = offset / opts.bso.block_size;
  const lbn_t lbn_end = (offset + size) / opts.bso.block_size;
  std::shared_ptr<Object> object;
  Buffer *buffer;
  IoRequest *request;
  std::list<Buffer *> all_buffers;
  std::list<Buffer *> non_uptodate_buffers;
  uint32_t buf_ptr;
  uint32_t unread;
  uint32_t bytes_to_read;
  uint32_t out_ptr;

  object = search_object(object_name, false);
  if (object == nullptr) {
    return os::OBJECT_NOT_FOUND;
  }

  if (size == 0) {
    return 0;
  }

  logger->debug(
    fmt::sprintf("[ObjectStore] get_object: object_name(%s) offset(%d) size(%d)\n",
    object_name.c_str(), offset, size));

  std::lock_guard<std::mutex> lock(object->mutex);

  // Get the buffers ready
  for (lbn_t lbn = lbn_start; 
       lbn <= lbn_end; 
       ++lbn) {
    buffer = get_block(object, lbn, nullptr, lbn_end, false, 0);

    if (buffer == nullptr) {
      return os::NO_CONTENT;
    }

    all_buffers.push_back(buffer);

    if (!flag_marked(buffer, B_UPTODATE)) {
      non_uptodate_buffers.push_back(buffer);
    }
  }

  if (!non_uptodate_buffers.empty()) {
    request = allocate_request(OP_READ);
    request->after_complete_callback = std::bind(&ObjectStore::after_read, 
      this, request, false);
    request->buffers = std::move(non_uptodate_buffers);

    block_store.submit_request(request);

    request->wait_for_complete();
    end_request(request);
    delete request;
  }

  // Read buffers into the space provided
  out_ptr = 0;
  unread = size;

  char buf[8192];
  for (Buffer *buffer: all_buffers) {
    buf_ptr = out_ptr == 0 ? offset % opts.bso.block_size: 0;
    bytes_to_read = std::min(opts.bso.block_size - buf_ptr, unread);

    buffer->copy_out(buf, 0, buf_ptr, bytes_to_read);

    out->append(buf, bytes_to_read);

    out_ptr += bytes_to_read;
    unread -= bytes_to_read;
  }

  assert(unread == 0);

  // Release the buffers
  for (Buffer *buffer: all_buffers) {
    buffer_manager.put_buffer(buffer);
  }

  logger->debug(fmt::sprintf("[ObjectStore] get_object: object_name(%s) offset(%d) size(%d)\n",
    object_name.c_str(), offset, size));

  return 0;
}

void ObjectStore::stop() {
  while (has_pending_operations()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    continue;
  }

  kv_store.stop();
  running = false;
  block_store.stop();
}

void ObjectStore::after_write(IoRequest *request, bool finished) {
  for (Buffer *buffer: request->buffers) {
    flag_unmark(buffer, B_DIRTY);
    buffer_manager.put_buffer(buffer);

    // It's locked when calling block_store.submit_request
    buffer->mutex.unlock();
  }

  if (finished) {
    end_request(request);
  } else {
    request->notify_complete();
  }
}

void ObjectStore::after_read(IoRequest *request, bool finished) {
  for (Buffer *buffer: request->buffers) {
    flag_mark(buffer, B_UPTODATE);
  }

  if (finished) {
    end_request(request);
  } else {
    request->notify_complete();
  }
}

int ObjectStore::put_metadata(const std::string &object_name, 
                              const std::string &attribute,
                              const bool create_object,
                              const std::string &value) {
  rocksdb::Status status;

  if (search_object(object_name, false) == nullptr) {
    if (create_object) {
      put_object(object_name, 0, "");
    } else {
      return os::OBJECT_NOT_FOUND;
    }
  }

  status = kv_store.put(CF_INDEX::CF_OBJ_META,
                        get_object_metadata_key(object_name, attribute),
                        value);

  return os::OPERATION_SUCCESS;
}

int ObjectStore::get_metadata(const std::string &object_name,
                              const std::string &attribute, std::string *buf) {
  rocksdb::Status status;

  if (search_object(object_name, false) == nullptr) {
    return os::OBJECT_NOT_FOUND;
  }

  status = kv_store.get(CF_INDEX::CF_OBJ_META,
                        get_object_metadata_key(object_name, attribute),
                        buf);

  if (!status.ok()) {
    if (status == status.NotFound()) {
      return METADATA_NOT_FOUND;
    }
    std::cerr << status.ToString() << std::endl;
    assert(0);
  }

  return OPERATION_SUCCESS;
}

int ObjectStore::delete_metadata(const std::string &object_name,
                                 const std::string &attribute) {
  rocksdb::Status status;

  if (search_object(object_name, false) == nullptr) {
    return OBJECT_NOT_FOUND;
  }

  status = kv_store.del(CF_INDEX::CF_OBJ_META,
                        get_object_metadata_key(object_name, attribute));

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    assert(0);
  }

  return OPERATION_SUCCESS;
}

} // namespace os

} // namespace morph
