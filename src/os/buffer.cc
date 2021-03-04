#include <os/buffer.h>

#include <spdlog/sinks/basic_file_sink.h>


namespace morph {

BufferManager::BufferManager(uint32_t total_buffers, std::shared_ptr<BlockStore> bs):
  TOTAL_BUFFERS(total_buffers),
  running(true),
  block_store(bs) {

  for (uint32_t i = 0; i < total_buffers; ++i) {
    free_list.push_back(std::make_shared<Buffer>());
  }

  io_thread = std::make_unique<std::thread>(&BufferManager::io, this);
}

BufferManager::~BufferManager() {
  io_requests.push(nullptr);
  running.store(false);
  io_thread->join();
}

void BufferManager::write(std::shared_ptr<Buffer> buffer, const void *buf, size_t size) {
  assert(size <= 512);

  std::lock_guard<std::mutex> lock(buffer->mutex);
  memcpy(buffer->data, buf, size);

  if (buffer->flag[Buffer::DIRTY] == 0) {
    io_requests.push(buffer);
    buffer->flag[Buffer::DIRTY] = 1;
    //fprintf(stderr, "SET bno(%d) addr(%p) to dirty\n", buffer->bno, buffer);
    buffer->flag[Buffer::UPTODATE] = 0;
  }
}

void BufferManager::read(std::shared_ptr<Buffer> buffer) {
  assert(buffer != nullptr);
  std::unique_lock<std::mutex> lock(buffer->mutex);

  if (buffer->flag[Buffer::UPTODATE]) {
    //fprintf(stderr, "[Buffer] bno(%d) is uptodate\n", buffer->bno);
    return;
  }

  //fprintf(stderr, "[Buffer] read bno(%d) reach here dirty(%d)\n", (bool)buffer->flag[Buffer::DIRTY]);

  buffer->flag[Buffer::READ_REQUESTED] = 1;

  if (buffer->flag[Buffer::DIRTY] == 0) {
    io_requests.push(buffer);
  }
  buffer->read_complete.wait(lock,
    [buffer]() {
      return buffer->flag[Buffer::DIRTY] == 0 && buffer->flag[Buffer::UPTODATE] == 1;
    });

  //fprintf(stderr, "[Buffer] read bno(%d) addr(%p): i am out bro dirty(%d) uptodate(%d)\n", 
  //  buffer, (bool)buffer->flag[Buffer::DIRTY], (bool)buffer->flag[Buffer::UPTODATE]);
}

std::shared_ptr<Buffer> BufferManager::get_buffer(bno_t bno) {
  std::lock_guard<std::mutex> lock(global_mutex);
  std::shared_ptr<Buffer> buffer;

  buffer = lookup_index(bno);
  if (buffer != nullptr) {
    free_list_remove(bno);
  } else {
    // Not present, so we need a buffer from free list
    buffer = free_list_pop();

    std::unique_lock<std::mutex> lock(buffer->mutex);
    if (buffer->flag[Buffer::VALID]) {
      index.erase(buffer->bno);

      if (buffer->flag[Buffer::DIRTY]) {
        buffer->write_complete.wait(lock,
          [&buffer]() {
            return buffer->flag[Buffer::DIRTY] == 0;
          });
      }
    }
     
    index.emplace(bno, buffer);

    buffer->init(bno);

    assert(buffer->flag[Buffer::DIRTY] == 0);
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

// Right now, only sync() is allowed to call this. Do not call this in other situations.
void BufferManager::sync_write_to_disk(std::shared_ptr<Buffer> buffer) {
  block_store->write_to_disk(buffer->bno, buffer->data);
}

void BufferManager::sync_read_from_disk(std::shared_ptr<Buffer> buffer) {
  block_store->read_from_disk(buffer->bno, buffer->data);
}

void BufferManager::io() {
  std::shared_ptr<Buffer> buffer;

  while (true) {
    if (!running) {
      if (io_requests.empty()) {
        break;
      }
    }

    buffer = io_requests.pop();

    // Signal that it's time to exit
    if (buffer == nullptr) {
      break;
    }

    std::unique_lock<std::mutex> lock(buffer->mutex);

    assert(buffer->flag[Buffer::VALID]);

    if (buffer->flag[Buffer::DIRTY]) {
      sync_write_to_disk(buffer);


      buffer->flag[Buffer::DIRTY] = 0;
      buffer->flag[Buffer::UPTODATE] = 1;

      //fprintf(stderr, "[Buffer] write is done bno(%d) dirty(%d)\n", buffer->bno,
      //  (bool)buffer->flag[Buffer::DIRTY]);

      buffer->write_complete.notify_one();

      if (buffer->flag[Buffer::READ_REQUESTED]) {
        buffer->flag[Buffer::READ_REQUESTED] = 0;
        //fprintf(stderr, "[Buffer] write allows read for bno(%d)\n", buffer->bno);
        buffer->read_complete.notify_all();
      }
    } else if (buffer->flag[Buffer::READ_REQUESTED]) {
      sync_read_from_disk(buffer);

      //fprintf(stderr, "[Buffer] read is done bno(%d)\n", buffer->bno);      
      buffer->flag[Buffer::UPTODATE] = 1;
      buffer->flag[Buffer::READ_REQUESTED] = 0;
      buffer->read_complete.notify_all();
    } else {
      assert(0);
    }
  }
}

}