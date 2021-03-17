#include <os/buffer.h>

#include <string>
#include <spdlog/sinks/basic_file_sink.h>


namespace morph {

BufferManager::BufferManager(BufferManagerOptions o):
  opts(o) {

  for (uint32_t i = 0; i < opts.TOTAL_BUFFERS; ++i) {
    free_list.push_back(std::make_shared<Buffer>(opts.BUFFER_SIZE));
  }
}

std::shared_ptr<Buffer> BufferManager::get_buffer(pbn_t pbn) {
  std::shared_ptr<Buffer> buffer;

  for (uint8_t retry = 0; retry < opts.ALLOCATE_RETRY; ++retry) {
    buffer = try_get_buffer(pbn);
    if (buffer != nullptr) {
      break;
    }

    // TODO: condition variable...
    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }

  //fprintf(stderr, "[Buffer] get_buffer pbn(%d) buffer->pbn(%d) DIRTY(%d) UPTODATE(%d)\n", 
  //  pbn, buffer->pbn, flag_marked(buffer, B_DIRTY), flag_marked(buffer, B_UPTODATE));

  return buffer;
}

std::shared_ptr<Buffer> BufferManager::try_get_buffer(pbn_t pbn) {
  std::shared_ptr<Buffer> buffer;
  std::lock_guard<std::mutex> lock(global_mutex);

  buffer = lookup_index(pbn);
  if (buffer != nullptr) {
    free_list_remove(pbn);
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
     
    index.emplace(pbn, buffer);

    buffer->init(pbn);
  }

  ++buffer->ref;

  return buffer;
}

void BufferManager::put_buffer(std::shared_ptr<Buffer> buffer) {
  std::lock_guard<std::mutex> lock(global_mutex);

  assert(buffer != nullptr);

  if (--buffer->ref == 0) {
    free_list_push(buffer);
  }
}

}