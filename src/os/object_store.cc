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

  flush_thread = std::make_unique<std::thread>(&ObjectStore::flush_routine, this);
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

  // TODO: Replay the logs??
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

  // When creating a new object, it's possible that it has no body. Like when filesystem open a new file.
  // Directory has no data throughout its lifetime.
  if (!data.empty()) {
    object_write(object, object_name, offset, data);
  }

  logger->debug(fmt::sprintf("[ObjectStore] w(%d) ow(%d) put_object: object_name(%s) offset(%d): success\n",
    --w_count, --object->w_cnt, object_name.c_str(), offset));

  return 0;
}

// Requires the object's mutex to be held.
void ObjectStore::object_write(std::shared_ptr<Object> object, const std::string &object_name,
                               const uint32_t offset, const std::string &data) {
  const lbn_t lbn_start = offset / opts.bso.BLOCK_SIZE;
  const lbn_t lbn_end = (offset + data.size()) / opts.bso.BLOCK_SIZE;
  std::shared_ptr<Buffer> buffer;
  uint32_t unwritten;
  uint32_t size;
  uint32_t buf_off;
  const char *data_ptr;
  uint32_t new_blocks;
  std::shared_ptr<LogHandle> handle;
  std::shared_ptr<WriteUnit> write_unit;
  bool bitmap_modified;

  std::lock_guard<std::mutex> lock(object->mutex);

  data_ptr = data.c_str();
  unwritten = data.size();
  new_blocks = 0;
  handle = kv_store.start_transaction();
  write_unit = std::make_shared<WriteUnit>(handle);
  bitmap_modified = false;

  for (lbn_t lbn = lbn_start; 
       lbn <= lbn_end; 
       ++lbn, unwritten -= size, data_ptr += size) {
    buffer = get_block(object, lbn, lbn_end, true, new_blocks);

    // When get_block tries to allocate, it will allocate more than one blocks
    // So all blocks following the first one should be marked B_NEW
    // We use this new_blocks to count how many blocks following the first one
    // should be marked as new_block
    if (flag_marked(buffer, B_NEW)) {
      bitmap_modified = true;

      if (new_blocks == 0) {
        new_blocks = lbn_end - lbn;
      } else {
        --new_blocks;
      }
    }

    {
      std::lock_guard<std::mutex> lock(buffer->mutex);

      if (!flag_marked(buffer, B_DIRTY)) {
        write_unit->dirty_buffers.push_back(buffer);
      }
    }

    buf_off = lbn == lbn_start ? offset % opts.bso.BLOCK_SIZE: 0;
    size = std::min(opts.bso.BLOCK_SIZE - buf_off, unwritten);
    write_buffer(buffer, data_ptr, buf_off, size);
  }

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

  if (!write_unit->dirty_buffers.empty()) {
    write_queue.push(write_unit);
  }

  kv_store.end_transaction(handle);
}

std::shared_ptr<Buffer> ObjectStore::get_block(std::shared_ptr<Object> object, lbn_t lbn, lbn_t lbn_end, 
                                               bool create, uint32_t new_blocks) {
  std::shared_ptr<Buffer> buffer;
  Extent extent;
  uint32_t count;    /* How many blocks to allcoate if ncessary */
  pbn_t pbn;
  bool allocated = false;

  if (object->search_extent(lbn, extent)) {
    pbn = extent.start_pbn + (lbn - extent.start_lbn);
  } else {
    if (!create) {
      return nullptr;
    }

    if (extent.valid()) {
      count = std::min(lbn_end - lbn + 1,
                        extent.start_lbn - lbn);
    } else {
      count = lbn_end - lbn + 1;
    }

    pbn = block_store.allocate_blocks(count);

    object->insert_extent(lbn, lbn + count - 1, pbn);

    allocated = true;
  }

  buffer = buffer_manager.get_buffer(pbn);

  if (allocated || new_blocks > 0) {
    flag_mark(buffer, B_NEW);
  }

  return buffer;
}

void ObjectStore::write_buffer(std::shared_ptr<Buffer> buffer, 
                               const char *data_ptr, uint32_t buf_offset, uint32_t size) {
  if (size < opts.bso.BLOCK_SIZE && !flag_marked(buffer, B_NEW)) {
    read_buffer(buffer);
  }

  buffer->copy_data(data_ptr, buf_offset, 0, size);

  flag_mark(buffer, B_DIRTY);
  flag_mark(buffer, B_UPTODATE);
  flag_unmark(buffer, B_NEW);
}

// TODO: is it a waste to allocate a request for a single buffer?
void ObjectStore::read_buffer(std::shared_ptr<Buffer> buffer) {
  std::shared_ptr<IoRequest> request;

  if (flag_marked(buffer, B_UPTODATE)) {
    return;
  }

  request = std::make_shared<IoRequest>(OP_READ);
  request->buffers.push_back(buffer);

  block_store.submit_io(request);

  request->wait_for_completion();
}

int ObjectStore::get_object(const std::string &object_name, std::string &out, const uint32_t offset, const uint32_t size) {
  const lbn_t lbn_start = offset / opts.bso.BLOCK_SIZE;
  const lbn_t lbn_end = (offset + size) / opts.bso.BLOCK_SIZE;
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

  request = std::make_shared<IoRequest>(OP_READ);

  std::lock_guard<std::mutex> lock(object->mutex);

  // Get the buffers ready
  for (lbn_t lbn = lbn_start; 
       lbn <= lbn_end; 
       ++lbn) {
    buffer = get_block(object, lbn, lbn_end, false, 0);

    if (buffer == nullptr) {
      return NO_CONTENT;
    }

    buffers.push_back(buffer);

    if (!flag_marked(buffer, B_UPTODATE)) {
      request->buffers.push_back(buffer);
    }
  }

  if (!request->buffers.empty()) {
    block_store.submit_io(request);
    request->wait_for_completion();
  }

  // Read buffers into the space provided
  out_ptr = 0;
  unread = size;

  for (const std::shared_ptr<Buffer> &buffer: buffers) {
    flag_mark(buffer, B_UPTODATE);

    buf_ptr = out_ptr == 0? offset % opts.bso.BLOCK_SIZE: 0;
    bytes_to_read = std::min(opts.bso.BLOCK_SIZE - buf_ptr, unread);

    out.append(buffer->buf + buf_ptr, bytes_to_read);

    out_ptr += bytes_to_read;
    unread -= bytes_to_read;
  }

  assert(unread == 0);

  // Release the buffers
  for (std::shared_ptr<Buffer> &buffer: buffers) {
    buffer_manager.put_buffer(buffer);
  }

  logger->debug(fmt::sprintf("[ObjectStore] r(%d) or(%d) get_object: object_name(%s) offset(%d) size(%d) result(%s)\n",
    --r_count, --object->r_cnt, object_name.c_str(), offset, size, out.c_str()));

  return 0;
}

// Flush dirty buffers
void ObjectStore::flush_routine() {
  std::shared_ptr<WriteUnit> write_unit;
  std::shared_ptr<IoRequest> request;
  bool has_dirty;
  std::list<std::shared_ptr<IoRequest>> submitted;

  while (running) {
    while (write_queue.try_pop(write_unit)) {
      request = std::make_shared<IoRequest>(OP_WRITE);

      request->buffers = std::move(write_unit->dirty_buffers);

      block_store.submit_io(request);
      submitted.push_back(request);
    }

    // Handling buffers after they have been written
    while (!submitted.empty() && submitted.front()->is_completed()) {
      request = submitted.front();
      submitted.pop_front();

      for (std::shared_ptr<Buffer> &buffer: request->buffers) {
        flag_unmark(buffer, B_DIRTY);

        buffer_manager.put_buffer(buffer);
      }
    }

    std::this_thread::yield();
  }
}

void ObjectStore::stop() {
  kv_store.stop();

  running = false;
  flush_thread->join();

  block_store.stop();
}

}