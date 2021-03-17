#include <os/object_store.h>

#include <cassert>
#include <common/utils.h>

namespace morph {

ObjectStore::ObjectStore(ObjectStoreOptions oso):
  opts(oso),
  block_store(oso.bso),
  buffer_manager(oso.bmo),
  running(true) {
  flush_thread = std::make_unique<std::thread>(&ObjectStore::flush, this);
}

ObjectStore::~ObjectStore() {
  running = false;
  flush_thread->join();
}

std::shared_ptr<Object> ObjectStore::search_object(const std::string &name, bool create) {
  std::shared_ptr<Object> object;

  std::lock_guard<std::mutex> lock(index_mutex);

  auto f = index.find(name);

  // If the object does not exist, create it
  if (f == index.end()) {
    if (!create) {
      return nullptr;
    }

    object = std::make_shared<Object>();
    index.emplace(name, object);
  } else {
    object = f->second;
  }

  return object;
}

int ObjectStore::put_object(const std::string &object_name, const uint32_t offset, const std::string &data) {
  std::shared_ptr<Object> object;

  if (object_name.empty()) {
    return OBJECT_NAME_INVALID;
  }

  object = search_object(object_name, true);

  // When creating a new object, it's possible that it has no body. Like when filesystem open a new file.
  // Directory has no data throughout its lifetime.
  if (!data.empty()) {
    object_write(object, offset, data);
  }

  return 0;
}

// Requires the object's mutex to be held.
void ObjectStore::object_write(std::shared_ptr<Object> object, const uint32_t offset, const std::string &data) {
  const lbn_t lbn_start = offset / opts.bso.BLOCK_SIZE;
  const lbn_t lbn_end = (offset + data.size()) / opts.bso.BLOCK_SIZE;
  std::shared_ptr<Buffer> buffer;
  uint32_t unwritten;
  uint32_t size;
  uint32_t buf_off;
  bool was_dirty = !object->dirty_buffers.empty();
  const char *data_ptr;
  uint32_t new_blocks;

  //fprintf(stderr, "[OS] object_write start_lbn(%d) end_lbn(%d) offset(%d) size(%d)\n", 
  //  lbn_start, lbn_end, offset, data.size());

  std::lock_guard<std::mutex> lock(object->mutex);

  data_ptr = data.c_str();
  unwritten = data.size();
  new_blocks = 0;

  for (lbn_t lbn = lbn_start; 
       lbn <= lbn_end; 
       ++lbn, unwritten -= size, data_ptr += size) {
    buffer = get_block(object, lbn, lbn_end, true, new_blocks);

    // When get_block tries to allocate, it will allocate more than one blocks
    // So all blocks following the first one should be marked B_NEW
    // We use this new_blocks to count how many blocks following the first one
    // should be marked as new_block
    if (flag_marked(buffer, B_NEW)) {
      if (new_blocks == 0) {
        new_blocks = lbn_end - lbn;
      } else {
        --new_blocks;
      }
    }

    //fprintf(stderr, "  [OS] object_write buffer lbn(%d) pbn(%d) DIRTY[%d] NEW[%d] UPTODATE[%d]\n", 
    //  lbn, buffer->pbn, flag_marked(buffer, B_DIRTY), flag_marked(buffer, B_NEW), flag_marked(buffer, B_UPTODATE));

    buf_off = lbn == lbn_start ? offset % opts.bso.BLOCK_SIZE: 0;
    size = std::min(opts.bso.BLOCK_SIZE - buf_off, unwritten);
    write_buffer(object, buffer, data_ptr, buf_off, size);
  
    object->dirty_buffers.push_back(buffer);

    //fprintf(stderr, "  [OS] object_write buffer lbn(%d) pbn(%d) is DIRTY and UPTODATE\n", lbn, buffer->pbn);
  }

  assert(unwritten == 0);

  if (!was_dirty) {
    dirty_objects.push(object);
  }
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
    //fprintf(stderr, "  [OS] get_block find match!! pbn(%d)\n", pbn);
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

    //fprintf(stderr, "  [OS] get_block allocated count(%d) blocks  pbn(%d)\n", count, pbn);
  }

  buffer = buffer_manager.get_buffer(pbn);

  if (allocated || new_blocks > 0) {
    flag_mark(buffer, B_NEW);
  }

  return buffer;
}

void ObjectStore::write_buffer(std::shared_ptr<Object> object, std::shared_ptr<Buffer> buffer, 
                                   const char *data_ptr, uint32_t buf_offset, uint32_t size) {
  if (size < opts.bso.BLOCK_SIZE && !flag_marked(buffer, B_NEW)) {
    read_buffer(object, buffer);
  }

  buffer->copy_data(data_ptr, buf_offset, 0, size);

  flag_mark(buffer, B_DIRTY);
  flag_mark(buffer, B_UPTODATE);
  flag_unmark(buffer, B_NEW);
}

// TODO: is it a waste to allocate a request for a single buffer?
void ObjectStore::read_buffer(std::shared_ptr<Object> object, std::shared_ptr<Buffer> buffer) {
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

    //fprintf(stderr, "  [OS] get_object copy a buffer pbn(%d) buf_ptr(%d) out_ptr(%d) bytes_to_read(%d) buffer_data(%s)\n", 
    //  buffer->pbn, buf_ptr, out_ptr, bytes_to_read, std::string(buffer->buf, opts.bmo.BUFFER_SIZE).c_str());
    out.append(buffer->buf + buf_ptr, bytes_to_read);

    out_ptr += bytes_to_read;
    unread -= bytes_to_read;
  }

  assert(unread == 0);

  // Release the buffers
  for (std::shared_ptr<Buffer> &buffer: buffers) {
    buffer_manager.put_buffer(buffer);
  }

  //fprintf(stderr, "[OS] get_object final result(%s)\n", out.c_str());

  return 0;
}

// Flush dirty buffers
void ObjectStore::flush() {
  std::shared_ptr<Object> object;
  std::shared_ptr<IoRequest> request;
  bool has_dirty;
  std::list<std::shared_ptr<IoRequest>> submitted;

  while (running) {
    while (dirty_objects.try_pop(object)) {
      request = std::make_shared<IoRequest>(OP_WRITE);

      {
        std::lock_guard<std::mutex> lock(object->mutex);

        if (object->dirty_buffers.empty()) {
          continue;
        }
 
        for (const std::shared_ptr<Buffer> &buffer: object->dirty_buffers) {
          request->buffers.push_back(buffer);
        }
      }

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

    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }
}

}