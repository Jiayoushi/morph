#include <os/object_store.h>

#include <cassert>
#include <common/utils.h>
#include <spdlog/fmt/bundled/printf.h>

namespace morph {

ObjectStore::ObjectStore(uint32_t idp, ObjectStoreOptions oso):
  opts(oso),
  block_store(opts.bso),
  kv_store(opts.kso),
  buffer_manager(oso.bmo),
  id(idp),
  w_count(0),
  r_count(0),
  running(true) {

  try {
    std::string filepath = LOGGING_DIRECTORY + "/oss_" + std::to_string(idp);
    logger = spdlog::basic_logger_mt("oss_" + std::to_string(idp) + std::to_string(rand()), filepath, true);
    logger->set_level(LOGGING_LEVEL);
    logger->flush_on(FLUSH_LEVEL);
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "Object storage service log init failed: " << ex.what() << std::endl;
    exit(EXIT_FAILURE);
  }

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
  s = kv_store.db->Get(rocksdb::ReadOptions(), kv_store.handles[CF_SYS_META], "system-bitmap", &bitmap);
  if (!s.ok()) {
    logger->warn("No system-bitmap in the kv store! recover procedure stopped.");
    return;
  }
  block_store.deserialize_bitmap(bitmap);

  // Restore object metadata
  auto p = kv_store.db->NewIterator(ReadOptions(), kv_store.handles[CF_OBJ_META]);
  for (p->SeekToFirst(); p->Valid(); p->Next()) {
    object_name = p->key().ToString();

    object = search_object(object_name, false);
    assert(object == nullptr);

    object = allocate_object(object_name);

    deserialize(p->value().ToString(), *object);
  }

  // TODO: Replay the data logs??
}

std::shared_ptr<Object> ObjectStore::allocate_object(const std::string &name) {
  std::shared_ptr<Object> object;

  std::lock_guard<std::recursive_mutex> lock(index_mutex);
 
  object = std::make_shared<Object>();
  index.emplace(name, object);

  return object;
}

std::shared_ptr<Object> ObjectStore::search_object(const std::string &name, bool create) {
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


int ObjectStore::put_object(const std::string &object_name, const uint32_t offset, const std::string &data) {
  std::shared_ptr<Object> object;

  if (object_name.empty()) {
    return OBJECT_NAME_INVALID;
  }

  object = search_object(object_name, true);

  logger->debug(fmt::sprintf("[ObjectStore] w(%d) ow(%d) put_object object_name(%s) offset(%d) data(%s)\n",
    ++w_count, ++object->w_cnt, object_name.c_str(), offset, data.c_str()));

  if (!data.empty()) {
    object_write(object, object_name, offset, data);
  }

  logger->debug(fmt::sprintf("[ObjectStore] w(%d) ow(%d) put_object: object_name(%s) offset(%d): success\n",
    --w_count, --object->w_cnt, object_name.c_str(), offset));

  return 0;
}

// rmw part
// cow part
// in the same io request
// first wait for the rmw is logged
// then after all data are written, time to change and log the metadata
// after the metadata are logged, 
void ObjectStore::object_write(std::shared_ptr<Object> object, const std::string &object_name, 
                               const uint32_t offset, const std::string &data) {
  const lbn_t lbn_start = offset / opts.bso.block_size;
  const lbn_t lbn_end = (offset + data.size()) / opts.bso.block_size;
  std::shared_ptr<Buffer> buffer;
  bool bitmap_modified = false;
  uint32_t unwritten = data.size();
  uint32_t write_len = 0, buf_off = 0, new_blocks = 0;
  const char *data_ptr = data.c_str();
  std::shared_ptr<IoRequest> request = allocate_io_request(object, OP_WRITE);

  std::lock_guard<std::mutex> lock(object->mutex);

  for (lbn_t lbn = lbn_start; 
       lbn <= lbn_end; 
       ++lbn, unwritten -= write_len, data_ptr += write_len) {
    buf_off = lbn == lbn_start ? offset % opts.bso.block_size: 0;
    write_len = std::min(opts.bso.block_size - buf_off, unwritten);

    buffer = get_block(object, lbn, lbn_end, true, new_blocks);

    //fprintf(stderr, "[OS] object_write: write get buffer %d ref %d dirty(%d)\n", 
    //  buffer->lbn, buffer->ref, flag_marked(buffer, B_DIRTY));

    // When get_block tries to allocate, it will allocate more than one blocks
    // So all blocks following the first one should be marked B_NEW
    // We use this new_blocks to count how many blocks following the first one
    // should be marked as new_block
    if (flag_marked(buffer, B_NEW)) {
      bitmap_modified = true;
      new_blocks = new_blocks == 0 ? lbn_end - lbn : new_blocks - 1;
    }

    write_buffer(object, buffer, data_ptr, buf_off, write_len);

    request->buffers.push_back(buffer);
  }

  assert(!request->buffers.empty());

  object->record_request(request);

  log_write(object, object_name, request, buffer, offset, bitmap_modified);
}

void ObjectStore::object_large_write(std::shared_ptr<Object> object, const std::string &object_name, 
                                     const uint32_t offset, const std::string &data) {
  const off_t start_blk = offset / opts.bso.block_size;
  const off_t end_blk = (offset + data.size()) / opts.bso.block_size;
  std::shared_ptr<Buffer> buffer;
  std::list<std::shared_ptr<Buffer>> rmw_buffers, cow_buffers;
  off_t cow_start_blk = start_blk, cow_end_blk = end_blk;
  uint32_t head_write_size = opts.bso.block_size - (offset % opts.bso.block_size);
  uint32_t tail_write_size = (data.size() - head_write_size) % opts.bso.block_size;
  lbn_t lbn;

  assert(start_blk != end_blk);

  // process the head block
  if (object->search_extent(start_blk, nullptr) 
      && offset % opts.bso.block_size == 0) {
    assert((buffer = get_block(object, start_blk)) != nullptr);
    rmw_buffers.push_back(buffer);
    ++cow_start_blk;
  }

  // process the tail block 
  if (object->search_extent(end_blk, nullptr) 
      && (offset + data.size()) % opts.bso.block_size == 0) {
    assert((buffer = get_block(object, end_blk)) != nullptr);
    rmw_buffers.push_back(buffer);
    --cow_end_blk;
  }

  lbn = block_store.allocate_blocks(cow_end_blk - cow_start_blk + 1);

  // write head block
  write_buffer(object, buffer, data.c_str(), 0, head_write_size);

  // write middle blocks
  for (lbn_t x = lbn; x < lbn + (cow_end_blk - cow_start_blk + 1); ++x) {
    buffer = buffer_manager.get_buffer(x);

    write_buffer(object, buffer, );   
  }

  // write tail block
  write_buffer(object, buffer, data.c_str(), );


  // TODO: 
  //std::vector<Extent> exts = object->search_extent(cow_start_blk, cow_end_blk);
  //for (Extent &ext: exts) {
    
  //}
  //object->insert_extent(cow_start_blk, cow_end_blk, lbn);
}

void ObjectStore::object_small_write(std::shared_ptr<Object> object, const std::string &object_name, 
                                     const uint32_t offset, const std::string &data) {

}

// Since we use aio, there is no ordering. Then we must be sure that this buffer is not submiited
// until the previous buffer write is completed. 
// We don't know whether this buffer is inside the open transaction or not
// we just assume it is and then force close it so that it can be immediately written
void ObjectStore::wait_dirty_buffer(const std::shared_ptr<Buffer> &buffer) {
  kv_store.force_close_open_txn();
  std::unique_lock<std::mutex> lock(buffer->mutex);
  buffer->write_complete.wait(lock, 
    [&buffer]() { 
      return !flag_marked(buffer, B_DIRTY); 
    });
}

void ObjectStore::log_write(const std::shared_ptr<Object> &object, const std::string &object_name, 
                            const std::shared_ptr<IoRequest> &request, const std::shared_ptr<Buffer> &buffer, 
                            uint32_t offset, bool bitmap_modified) {
  std::shared_ptr<LogHandle> handle;

  handle = kv_store.start_transaction();
  handle->post_log_callback = std::bind(&ObjectStore::post_log, this, request);

  if (bitmap_modified) {
    handle->log(LOG_SYS_META,
                std::move(std::string("system-bitmap")),
                std::move(block_store.serialize_bitmap()));
                
    std::string v = serialize(*object);
    handle->log(LOG_OBJ_META, 
                std::move(std::string(object_name)), 
                std::move(v));
  }

  handle->log(LOG_OBJ_DATA, 
              std::move(get_data_key(handle->transaction->id, object_name, offset)), 
              std::move(std::string(buffer->buf, buffer->buffer_size)));

  kv_store.end_transaction(handle);
}


// TODO: FIX THIS
std::shared_ptr<Buffer> ObjectStore::get_block(std::shared_ptr<Object> object, off_t target, off_t target_end, 
                                               bool create, uint32_t new_blocks) {
  std::shared_ptr<Buffer> buffer;
  Extent extent;
  uint32_t count;    /* How many blocks to allcoate if ncessary */
  lbn_t lbn;
  bool allocated = false;

  if (object->search_extent(target, &extent)) {
    lbn = extent.lbn + (target - extent.start);
  } else {
    if (!create) {
      return nullptr;
    }

    if (extent.valid()) {
      count = std::min(target_end - target + 1,
                        extent.start - target);
    } else {
      count = target_end - target + 1;
    }

    lbn = block_store.allocate_blocks(count);
    object->insert_extent(target, target + count - 1, lbn);
    allocated = true;
  }

  buffer = buffer_manager.get_buffer(lbn);

  if (allocated || new_blocks > 0) {
    flag_mark(buffer, B_NEW);
  }

  return buffer;
}

void ObjectStore::write_buffer(const std::shared_ptr<Object> &object, std::shared_ptr<Buffer> buffer, 
                               const char *data_ptr, uint32_t buf_offset, uint32_t size) {
  if (flag_marked(buffer, B_DIRTY)) {
    wait_dirty_buffer(buffer);
  }

  if (size < opts.bso.block_size && !flag_marked(buffer, B_NEW)) {
    read_buffer(object, buffer);
  }

  buffer->copy_data(data_ptr, buf_offset, 0, size);

  //fprintf(stderr, "[OS] write_buffer: attempt to mark %d as dirty\n", buffer->lbn);
  flag_mark(buffer, B_DIRTY);
  flag_mark(buffer, B_UPTODATE);
  flag_unmark(buffer, B_NEW);
}

// TODO: is it a waste to allocate a request for a single buffer?
//       should get all the buffers ready before anything else!
void ObjectStore::read_buffer(const std::shared_ptr<Object> &object, std::shared_ptr<Buffer> buffer) {
  std::shared_ptr<IoRequest> request;

  if (flag_marked(buffer, B_UPTODATE)) {
    return;
  }

  request = allocate_io_request(object, OP_READ);
  request->buffers.push_back(buffer);

  //fprintf(stderr, "[OS] read_buffer: push_request lbn(%d) size(%lu)\n",
  //  buffer->lbn, 1);
  block_store.push_request(request);

  request->wait_for_completion();
}

int ObjectStore::get_object(const std::string &object_name, std::string &out, const uint32_t offset, const uint32_t size) {
  const lbn_t lbn_start = offset / opts.bso.block_size;
  const lbn_t lbn_end = (offset + size) / opts.bso.block_size;
  std::shared_ptr<Object> object;
  std::shared_ptr<Buffer> buffer;
  std::shared_ptr<IoRequest> request;
  std::list<std::shared_ptr<Buffer>> buffers;
  uint32_t buf_ptr;
  uint32_t unread;
  uint32_t bytes_to_read;
  uint32_t out_ptr;

  object = search_object(object_name, false);
  if (object == nullptr) {
    return OBJECT_NOT_EXISTS;
  }

  if (size == 0) {
    return 0;
  }

  logger->debug(fmt::sprintf("[ObjectStore] r(%d) or(%d) get_object: object_name(%s) offset(%d) size(%d)\n",
    ++r_count, ++object->r_cnt, object_name.c_str(), offset, size));

  request = allocate_io_request(object, OP_READ);

  std::lock_guard<std::mutex> lock(object->mutex);

  // Get the buffers ready
  for (lbn_t lbn = lbn_start; 
       lbn <= lbn_end; 
       ++lbn) {
    buffer = get_block(object, lbn, lbn_end, false, 0);

    //fprintf(stderr, " get read buffer %d ref %d\n", buffer->lbn, buffer->ref);

    if (buffer == nullptr) {
      return NO_CONTENT;
    }

    buffers.push_back(buffer);

    if (!flag_marked(buffer, B_UPTODATE)) {
      request->buffers.push_back(buffer);
    }
  }

  if (!request->buffers.empty()) {
    ///fprintf(stderr, "[OS] get_object: push_request lbn(%d) size(%lu)\n",
    //  request->buffers.front()->lbn, request->buffers.size());
    block_store.push_request(request);
    request->wait_for_completion();
  }

  // Read buffers into the space provided
  out_ptr = 0;
  unread = size;

  for (const std::shared_ptr<Buffer> &buffer: buffers) {
    buf_ptr = out_ptr == 0? offset % opts.bso.block_size: 0;
    bytes_to_read = std::min(opts.bso.block_size - buf_ptr, unread);

    out.append(buffer->buf + buf_ptr, bytes_to_read);

    out_ptr += bytes_to_read;
    unread -= bytes_to_read;
  }

  assert(unread == 0);

  // Release the buffers
  for (std::shared_ptr<Buffer> &buffer: buffers) {
    //fprintf(stderr, "read release buffer %d ref %d\n", buffer->lbn, buffer->ref);
    buffer_manager.put_buffer(buffer);
  }

  logger->debug(fmt::sprintf("[ObjectStore] r(%d) or(%d) get_object: object_name(%s) offset(%d) size(%d) result(%s)\n",
    --r_count, --object->r_cnt, object_name.c_str(), offset, size, out.c_str()));

  return 0;
}

void ObjectStore::stop() {
  kv_store.stop();

  running = false;
  //flush_thread->join();

  block_store.stop();
}

void ObjectStore::post_write(const std::shared_ptr<Object> owner, const std::shared_ptr<IoRequest> &request) {
  // Important: remove the request should be placed before waking up any processes waiting for the buffers
  owner->remove_request(request);

  for (const std::shared_ptr<Buffer> &buffer: request->buffers) {
      //fprintf(stderr, "[OS] post_write: attempt to unmark %d dirty bit. request first lbn(%d)\n", 
      //  buffer->lbn, request->buffers.front()->lbn);
      flag_unmark(buffer, B_DIRTY);
      {
        std::unique_lock<std::mutex> lock(buffer->mutex);
        buffer->write_complete.notify_one();
      }
      buffer_manager.put_buffer(buffer);
  }
}

std::shared_ptr<IoRequest> ObjectStore::allocate_io_request(const std::shared_ptr<Object> owner, IoOperation op) {
  std::shared_ptr<IoRequest> req;

  req = std::make_shared<IoRequest>(op);
  if (op == OP_WRITE) {
    req->post_complete_callback = std::bind(&ObjectStore::post_write, this, owner, req);
  } else {
    req->post_complete_callback = std::bind(&ObjectStore::post_read, this, req);
  }

  return req;
}


}