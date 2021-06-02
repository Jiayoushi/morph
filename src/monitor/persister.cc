#include "persister.h"

#include <iostream>

namespace morph {
namespace paxos {

void Persister::write_to_disk() {
  if (ftruncate(fd, 0) < 0) {
    perror("ftruncate");
    assert(false);
  }
  if (lseek(fd, 0, SEEK_SET) < 0) {
    perror("lseek");
    assert(false);
  }

  const char *data = persisted.c_str();
  size_t size = persisted.size();

  while (size > 0) {
    ssize_t write_result = ::write(fd, data, size);
    if (write_result < 0) {
      if (errno == EINTR) {
        continue;
      }
      perror("write");
      assert(false);
    }
    data += write_result;
    size -= write_result;
  }
}

void Persister::read_from_disk() {
  if (lseek(fd, 0, SEEK_SET) < 0) {
    perror("lseek");
    assert(false);
  }

  const int BUF_SIZE = 4096;
  char buf[BUF_SIZE];
  while (true) {
    ::ssize_t read_size = ::read(fd, buf, BUF_SIZE);
    if (read_size < 0) {
      if (errno == EINTR) {
        continue;
      }
      perror("read");
      assert(false);
    }
    if (read_size == 0) {
      break;
    }
    persisted.append(buf, read_size);
  }
}


} // namespace paxos
} // namespace morph