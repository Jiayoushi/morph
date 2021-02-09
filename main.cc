#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <assert.h>
#include <iostream>

int main() {
  int fd = open("/dev/nvme0n1p9", O_RDWR);
  if (fd < 0) {
    perror(nullptr);
    exit(EXIT_FAILURE);
  }

  const char *data = "hello world!";
  int written = pwrite(fd, data, strlen(data), 0);
  if (written < 0) {
    perror(nullptr);
    exit(EXIT_FAILURE);
  }

  std::cout << "written: " << written << std::endl;

  char buf[100];
  int reads = pread(fd, buf, strlen(data), 0);
  if (reads < 0) {
    perror(nullptr);
    exit(EXIT_FAILURE);
  }

  std::cout << "read: " << reads << std::endl;

  buf[reads] = '\0';
  std::cout << "[" << buf << "]" << std::endl;  

  assert(strncmp(data, buf, sizeof(data)) == 0);

  close(fd);
  return 0;
}
