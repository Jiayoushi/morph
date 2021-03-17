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

  fd = open(opts.STORE_FILE.c_str(), O_CREAT | O_RDWR | O_DIRECT | O_TRUNC | O_SYNC);
  if (fd < 0) {
    perror("failed to open file to store buffers");
    exit(EXIT_FAILURE);
  }

  bitmap = std::make_unique<Bitmap>(opts.TOTAL_BLOCKS, opts.ALLOCATE_RETRY);

  io_thread = std::make_unique<std::thread>(&BlockStore::io, this);
}

BlockStore::~BlockStore() {
  io_requests.push(nullptr);
  io_thread->join();
  close(fd);
}

pbn_t BlockStore::allocate_blocks(uint32_t count) {
  return bitmap->allocate_blocks(count);
}

void BlockStore::free_blocks(pbn_t start_block, uint32_t count) {
  bitmap->free_blocks(start_block, count);
}

void BlockStore::write_to_disk(pbn_t pbn, const char *data) {
  ssize_t written = 0;

  // TODO: I don't know if this lock is necessary or not... need to figure it out later
  std::lock_guard<std::mutex> lock(rw);

  //fprintf(stderr, "[DISK] write pbn(%d) data(%s)\n", pbn, std::string(data, opts.BLOCK_SIZE).c_str());

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

  //fprintf(stderr, "[DISK] write pbn(%d) done\n", pbn);
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

  //fprintf(stderr, "[DISK] read pbn(%d) data(%s)\n", pbn, std::string(data, BLOCK_SIZE).c_str());
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

  while (true) {
    if (!running) {
      if (io_requests.empty()) {
        break;
      }
    }

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

}