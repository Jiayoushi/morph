#include <os/block_store.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <map>
#include <iostream>
#include <string.h>

namespace morph {

BlockStore::BlockStore(BlockStoreOptions o):
    opts(o),
    running(true) {
  int oflag;

  oflag = O_CREAT | O_RDWR | O_DIRECT | O_SYNC;
  if (!o.recover) {
    oflag |= O_TRUNC;
  }

  fd = open(opts.STORE_FILE.c_str(), oflag);
  if (fd < 0) {
    perror("failed to open file to store buffers");
    exit(EXIT_FAILURE);
  }

  bitmap = std::make_unique<Bitmap>(opts.TOTAL_BLOCKS, opts.ALLOCATE_RETRY);

  io_thread = std::make_unique<std::thread>(&BlockStore::io, this);
}

BlockStore::~BlockStore() {
  if (running) {
    stop();
  }

  if (close(fd) < 0) {
    perror("failed to close block store file");
    exit(EXIT_FAILURE);
  }
}

pbn_t BlockStore::allocate_blocks(uint32_t count) {
  return bitmap->allocate_blocks(count);
}

void BlockStore::free_blocks(pbn_t start_block, uint32_t count) {
  bitmap->free_blocks(start_block, count);
}

// TODO: should it be better to use writev?
//       how does DIRECT_IO work under the hood, should BLOCK_SIZE be set accordingly?
void BlockStore::write_to_disk(pbn_t pbn, const char *data) {
  ssize_t written = 0;

  // TODO: I don't know if this lock is necessary or not... need to figure it out later
  std::lock_guard<std::mutex> lock(rw);

  while (written != opts.BLOCK_SIZE) {
    written = pwrite(fd, data, opts.BLOCK_SIZE, pbn * opts.BLOCK_SIZE);
    if (written < 0) {
      perror("Failed to write");
      exit(EXIT_FAILURE);
    } else if (written != opts.BLOCK_SIZE) {
      std::cerr << "Partial write: " << written << std::endl;
      exit(EXIT_FAILURE);
    }
  }
}

void BlockStore::read_from_disk(pbn_t pbn, char *data) {
  ssize_t read = 0;

  std::lock_guard<std::mutex> lock(rw);

  while (read != opts.BLOCK_SIZE) {
    read = pread(fd, data, opts.BLOCK_SIZE, pbn * opts.BLOCK_SIZE);
    if (read < 0) {
      perror("Failed to read");
      exit(EXIT_FAILURE);
    } else if (read != opts.BLOCK_SIZE) {
      std::cerr << "Partial read: " << pbn << " " << read << std::endl;
      exit(EXIT_FAILURE);
    }
  }
}

void BlockStore::submit_io(std::shared_ptr<IoRequest> request) {
  io_requests.push(request);
}

void BlockStore::do_io(const std::shared_ptr<Buffer> &buffer, IoOperation op) {
  std::unique_lock<std::mutex> lock(buffer->mutex);

  if (op == OP_WRITE) {
    if (!flag_marked(buffer, B_DIRTY)) {
      return;
    }

    write_to_disk(buffer->pbn, buffer->buf);
  } else {
    if (flag_marked(buffer, B_UPTODATE)) {
      return;
    }

    if (flag_marked(buffer, B_DIRTY)) {
      write_to_disk(buffer->pbn, buffer->buf);
      return;
    }

    read_from_disk(buffer->pbn, buffer->buf);
  }
}

void BlockStore::io() {
  std::shared_ptr<IoRequest> request;

  while (running) {
    request = io_requests.pop();

    // Signal that it's time to exit
    if (request == nullptr) {
      break;
    }

    assert(request->status == IO_IN_PROGRESS);

    for (const std::shared_ptr<Buffer> &buffer: request->buffers) {
      do_io(buffer, request->op);
    }

    // At the moment, There should be only one process waiting for a io request
    request->status = IO_COMPLETED;
    request->io_complete.notify_one();
  }
}

void BlockStore::stop() {
  while (!io_requests.empty()) {
    std::this_thread::yield();
  }

  running = false;
  io_requests.push(nullptr);

  io_thread->join();
}

}