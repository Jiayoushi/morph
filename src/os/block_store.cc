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

bno_t BlockStore::get_block() {
  return bitmap->get_free_block();
}

void BlockStore::put_block(bno_t bno) {
  bitmap->put_block(bno);
}

void BlockStore::write_to_disk(bno_t bno, const char *data) {
  ssize_t written = 0;

  // TODO: I don't know if this lock is necessary or not... need to figure it out later
  std::lock_guard<std::mutex> lock(rw);
  //fprintf(stderr, "ACTUAL WRITE bno(%d)  addr(%p) data(%s)\n\n", bno, data, std::string(data, 512).c_str());

  while (written != 512) {
    written = pwrite(fd, data, 512, bno * 512);
    if (written < 0) {
      perror("Failed to write");
      exit(EXIT_FAILURE);
    } else if (written != 512) {
      std::cerr << "Partial write: " << written << std::endl;
      exit(EXIT_FAILURE);
    }
  }
}

void BlockStore::read_from_disk(bno_t bno, char *data) {
  ssize_t read = 0;

  std::lock_guard<std::mutex> lock(rw);

  while (read != 512) {
    read = pread(fd, data, 512, bno * 512);
    if (read < 0) {
      perror("Failed to read");
      exit(EXIT_FAILURE);
    } else if (read != 512) {
      std::cerr << "Partial read: " << bno << " " << read << std::endl;
      exit(EXIT_FAILURE);
    }
  }
}

}