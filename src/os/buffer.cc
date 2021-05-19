#include <os/buffer.h>

#include <string>
#include <spdlog/sinks/basic_file_sink.h>


namespace morph {

namespace os {

void Buffer::copy_data(const char *data, uint32_t buf_offset, 
    uint32_t data_offset, uint32_t size) {
  std::lock_guard<std::mutex> lock(mutex);
  memcpy(buf + buf_offset, data + data_offset, size);
}

BufferManager::BufferManager():
    BufferManager(BufferManagerOptions())
{}

BufferManager::~BufferManager() {
  assert(free_list.size() == opts.TOTAL_BUFFERS);

  for (Buffer *buffer: free_list) {
    delete buffer;
  }
}

BufferManager::BufferManager(BufferManagerOptions o):
    opts(o) {
  for (uint32_t i = 0; i < opts.TOTAL_BUFFERS; ++i) {
    free_list.push_back(new Buffer(opts.BUFFER_SIZE));
  }
}

Buffer * BufferManager::get_buffer(lbn_t lbn) {
  Buffer * buffer;

  while (true) {
    buffer = try_get_buffer(lbn);
    if (buffer != nullptr) {
      break;
    }

    // TODO: currently buffer manager waits for the dirty buffer to be flushed...
    //       or if all buffers are being used, simply wait here.

    // TODO: condition variable...
    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }

  return buffer;
}

Buffer * BufferManager::try_get_buffer(lbn_t lbn) {
  Buffer * buffer;
  std::lock_guard<std::mutex> lock(global_mutex);

  buffer = lookup_index(lbn);
  if (buffer != nullptr) {
    if (buffer->ref == 0) {
      free_list_remove(lbn);
    }
  } else {
    if (free_list.empty()) {
      return nullptr;
    }

    // Not present, so we need a buffer from free list
    buffer = free_list_pop();

    // Either there are no free buffers, or all the free buffers are currently dirty
    if (buffer == nullptr) {
      return nullptr;
    }

    assert(!flag_marked(buffer, B_DIRTY));
    
    if (flag_marked(buffer, B_VALID)) {
      index.erase(buffer->lbn);
    }

    index.emplace(lbn, buffer);

    buffer->init(lbn);
  }

  ++buffer->ref;

  return buffer;
}

void BufferManager::put_buffer(Buffer * buffer) {
  std::lock_guard<std::mutex> lock(global_mutex);

  assert(buffer != nullptr);

  uint32_t before = buffer->ref;

  --buffer->ref;
  if (buffer->ref == 0) {
    free_list_push(buffer);
  }
}

} // namespace os

} // namespace morph
