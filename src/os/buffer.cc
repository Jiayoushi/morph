#include <os/buffer.h>

#include <string>
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

void BufferManager::write(std::shared_ptr<Buffer> buffer, const char *buf, size_t size) {
  assert(size <= BLOCK_SIZE);

  std::lock_guard<std::mutex> lock(buffer->mutex);
  memcpy(buffer->data, buf, size);

  if (buffer->flag[Buffer::DIRTY] == 0) {
    io_requests.push(buffer);
    buffer->flag[Buffer::DIRTY] = 1;
    buffer->flag[Buffer::UPTODATE] = 0;
  }

  //fprintf(stderr, "[BUFFER] WRITE [%d] [%s]\n", buffer->pbn, std::string(buf, size).c_str());
}

void BufferManager::read(std::shared_ptr<Buffer> buffer) {
  assert(buffer != nullptr);
  std::unique_lock<std::mutex> lock(buffer->mutex);

  if (buffer->flag[Buffer::UPTODATE]) {
    return;
  }

  buffer->flag[Buffer::READ_REQUESTED] = 1;

  if (buffer->flag[Buffer::DIRTY] == 0) {
    io_requests.push(buffer);
  }
  buffer->read_complete.wait(lock,
    [buffer]() {
      return buffer->flag[Buffer::DIRTY] == 0 && buffer->flag[Buffer::UPTODATE] == 1;
    });
  //fprintf(stderr, "[BUFFER] read [%d] [%s]\n", buffer->pbn, std::string(buffer->data, morph::BLOCK_SIZE).c_str());
}

std::shared_ptr<BufferGroup> BufferManager::get_blocks(sector_t pbn, uint32_t count) {
  std::shared_ptr<BufferGroup> group;
  std::shared_ptr<Buffer> buffer;

  group = std::make_shared<BufferGroup>();
  while (true) {
    {
      std::lock_guard<std::mutex> lock(global_mutex);

      if (free_list.size() >= count) {
        for (uint32_t b = pbn; b < pbn + count; ++b) {
          buffer = get_block(b);
          group->buffers.push_back(buffer);
        }
        break;
      }
    }

    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }

  return group;
}

std::shared_ptr<Buffer> BufferManager::get_block(sector_t pbn) {
  std::shared_ptr<Buffer> buffer;

  buffer = lookup_index(pbn);
  if (buffer != nullptr) {
    free_list_remove(pbn);
  } else {
    // Not present, so we need a buffer from free list
    buffer = free_list_pop();

    std::unique_lock<std::mutex> lock(buffer->mutex);
    if (buffer->flag[Buffer::VALID]) {
      index.erase(buffer->pbn);

      if (buffer->flag[Buffer::DIRTY]) {
        buffer->write_complete.wait(lock,
          [&buffer]() {
            return buffer->flag[Buffer::DIRTY] == 0;
          });
      }
    }
     
    index.emplace(pbn, buffer);

    buffer->init(pbn);

    assert(buffer->flag[Buffer::DIRTY] == 0);
  }

  ++buffer->ref;

  if (buffer->flag[Buffer::VALID] == 0) {
    read(buffer);
  }

  return buffer;
}

void BufferManager::put_blocks(std::shared_ptr<BufferGroup> group) {
  std::lock_guard<std::mutex> lock(global_mutex);

  for (auto &p: group->buffers) {
    put_block(p);
  }
}

void BufferManager::put_block(std::shared_ptr<Buffer> buffer) {
  assert(buffer != nullptr);

  if (--buffer->ref == 0) {
    free_list_push(buffer);
  }
}

// Right now, only sync() is allowed to call this. Do not call this in other situations.
void BufferManager::sync_write_to_disk(std::shared_ptr<Buffer> buffer) {
  block_store->write_to_disk(buffer->pbn, buffer->data);
}

void BufferManager::sync_read_from_disk(std::shared_ptr<Buffer> buffer) {
  block_store->read_from_disk(buffer->pbn, buffer->data);
}

// TODO: this belong to the block layer... need to be refactored.
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

      //fprintf(stderr, "[Buffer] write is done pbn(%d) dirty(%d)\n", buffer->pbn,
      //  (bool)buffer->flag[Buffer::DIRTY]);

      buffer->write_complete.notify_one();

      if (buffer->flag[Buffer::READ_REQUESTED]) {
        buffer->flag[Buffer::READ_REQUESTED] = 0;
        //fprintf(stderr, "[Buffer] write allows read for pbn(%d)\n", buffer->pbn);
        buffer->read_complete.notify_all();
      }
    } else if (buffer->flag[Buffer::READ_REQUESTED]) {
      sync_read_from_disk(buffer);

      //fprintf(stderr, "[Buffer] read is done pbn(%d)\n", buffer->pbn);      
      buffer->flag[Buffer::UPTODATE] = 1;
      buffer->flag[Buffer::READ_REQUESTED] = 0;
      buffer->read_complete.notify_all();
    } else {
      assert(0);
    }
  }
}

}