#include <os/block_store.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <map>
#include <iostream>
#include <string.h>

namespace morph {

BlockStore::BlockStore(const std::string &filename, uint32_t total_blocks) {
  char *buffer;

  fd = open(filename.c_str(), O_CREAT | O_RDWR | O_DIRECT | O_TRUNC | O_SYNC);
  if (fd < 0) {
    perror("failed to open file to store buffers");
    exit(EXIT_FAILURE);
  }

  bitmap = std::make_unique<Bitmap>(total_blocks);
}

BlockStore::~BlockStore() {
  close(fd);
}

sector_t BlockStore::allocate_blocks(uint32_t count) {
  return bitmap->allocate_blocks(count);
}

void BlockStore::free_blocks(sector_t start_block, uint32_t count) {
  bitmap->free_blocks(start_block, count);
}

void BlockStore::write_to_disk(sector_t pbn, const char *data) {
  ssize_t written = 0;

  // TODO: I don't know if this lock is necessary or not... need to figure it out later
  std::lock_guard<std::mutex> lock(rw);

  while (written != BLOCK_SIZE) {
    written = pwrite(fd, data, BLOCK_SIZE, pbn * BLOCK_SIZE);
    if (written < 0) {
      perror("Failed to write");
      exit(EXIT_FAILURE);
    } else if (written != BLOCK_SIZE) {
      std::cerr << "Partial write: " << written << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  //fprintf(stderr, "[DISK] write pbn(%d) data(%s)\n", pbn, std::string(data, BLOCK_SIZE).c_str());
}

void BlockStore::read_from_disk(sector_t pbn, char *data) {
  ssize_t read = 0;

  std::lock_guard<std::mutex> lock(rw);

  while (read != BLOCK_SIZE) {
    read = pread(fd, data, BLOCK_SIZE, pbn * BLOCK_SIZE);
    if (read < 0) {
      perror("Failed to read");
      exit(EXIT_FAILURE);
    } else if (read != BLOCK_SIZE) {
      std::cerr << "Partial read: " << pbn << " " << read << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  //fprintf(stderr, "[DISK] read pbn(%d) data(%s)\n", pbn, std::string(data, BLOCK_SIZE).c_str());
}

}