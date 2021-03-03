#include <os/buffer.h>

#include <spdlog/sinks/basic_file_sink.h>


namespace morph {

BufferManager::BufferManager(uint32_t total_buffers, std::shared_ptr<BlockStore> bs):
  block_store(bs) {

  for (uint32_t i = 0; i < total_buffers; ++i) {
    free_buffers.push_back(std::make_shared<Buffer>());
  }
}

void BufferManager::write(std::shared_ptr<Buffer> buffer, const void *buf, size_t size) {
  assert(size <= 512);
  memcpy(buffer->data, buf, size);

  if (buffer->flag[Buffer::DIRTY] == 0) {
    dirty_buffers.push(buffer);
  }

  buffer->flag[Buffer::DIRTY] = 1;
  buffer->flag[Buffer::UPTODATE] = 0;
}

void BufferManager::read(std::shared_ptr<Buffer> buffer) {
  assert(buffer != nullptr);

  // TODO: what do we do with dirty? what's the semantics requirement?
  assert(buffer->flag[Buffer::DIRTY] == 0);

  if (buffer->flag[Buffer::UPTODATE] == 1) {
    //fprintf(stderr, "%d is uptodate !\n", buffer->bno);
    return;
  }

  sync_read_from_disk(buffer);  
}

std::shared_ptr<Buffer> BufferManager::get_buffer(bno_t bno) {
  std::shared_ptr<Buffer> buffer;

  auto p = index.find(bno);
  if (p != index.end()) {
    //printf(stderr, "Get buffer from index\n");

    buffer = p->second;

    // If the buffer is in free list, we need to remove it from the free list
    free_buffers.remove_if([bno](std::shared_ptr<Buffer> buf){return buf->bno == bno;});
  } else {
    // Not present, so we need a buffer from free list
    // TODO: needs to be blocking
    assert(!free_buffers.empty());

    buffer = free_buffers.back();
    free_buffers.pop_back();

    //fprintf(stderr, "Get buffer from free_list former is %d\n", buffer->bno);

    // TODO: async write?
    if (buffer->flag[Buffer::DIRTY]) {
      sync();
    }
    index.erase(buffer->bno);
    index.emplace(bno, buffer);

    buffer->reset();
    buffer->bno = bno;
  }

  ++buffer->ref;

  return buffer;
}

void BufferManager::put_buffer(std::shared_ptr<Buffer> buffer) {
  assert(buffer != nullptr);

  if (--buffer->ref == 0) {
    free_buffers.push_front(buffer);
  }
}

// Right now, only sync() is allowed to call this. Do not call this in other situations.
void BufferManager::sync_write_to_disk(std::shared_ptr<Buffer> buffer) {
  block_store->write_to_disk(buffer->bno, buffer->data);
  
  buffer->flag[Buffer::DIRTY] = 0;
  buffer->flag[Buffer::UPTODATE] = 1;
}

void BufferManager::sync_read_from_disk(std::shared_ptr<Buffer> buffer) {
  block_store->read_from_disk(buffer->bno, buffer->data);

  buffer->flag[Buffer::UPTODATE] = 1;
}

void BufferManager::sync() {
  std::unordered_set<bno_t> s;

  std::shared_ptr<Buffer> buffer;

  while (!dirty_buffers.empty()) {
    buffer = dirty_buffers.pop();
    sync_write_to_disk(buffer);

    if (s.find(buffer->bno) != s.end()) {
      std::cout << " ??? " << buffer->bno << std::endl;
      assert(0);
    }
    s.insert(buffer->bno);
  }
}

}