// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// 
// Code modified.

#include "env.h"

#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string>

#include "utils.h"

namespace morph {

constexpr const size_t WRITABLE_FILE_BUFFER_SIZE = 2048;

SequentialFile::~SequentialFile() {}

WritableFile::~WritableFile() {}

class PosixSequentialFile final: public SequentialFile {
 public:
  PosixSequentialFile(std::string filename, int fd):
      fd(fd), filename(filename)
  {}

  ~PosixSequentialFile() override {
    close(fd);
  }

  Status read(size_t n, Slice *result, char *scratch) override {
    Status status;
    while (true) {
      ::size_t read_size = ::read(fd, scratch, n);
      if (read_size < 0) {
        if (errno == EINTR) {
          continue;
        }
        status = posix_error(filename, errno);
        break;
      }
      *result = Slice(scratch, read_size);
      break;
    }
    return status;
  }

  Status skip(uint64_t n) override {
    if (::lseek(fd, n, SEEK_CUR) == static_cast<off_t>(-1)) {
      return posix_error(filename, errno);
    }
    return Status::ok();
  }

 private:
  const int fd;
  const std::string filename;
};


class PosixWritableFile final: public WritableFile {
 public:
  PosixWritableFile(std::string filename, int fd):
    pos(0), fd(fd), filename(filename)
  {}

  ~PosixWritableFile() override {
    if (fd >= 0) {
      close();
    }
  }

  Status append(const Slice &data) override {
    size_t write_size = data.size();
    const char *write_data = data.data();

    // Fit as much as possible into buffer.
    size_t copy_size = std::min(write_size, WRITABLE_FILE_BUFFER_SIZE);
    std::memcpy(buf + pos, write_data, copy_size);
    write_data += copy_size;
    write_size -= copy_size;
    pos += copy_size;
    if (write_size == 0) {
      return Status::ok();
    }
    
    // Can't fit in buffer, so need to do at least one write
    Status status = flush_buffer();
    if (!status.is_ok()) {
      return status;
    }

    // Small writes go to buffer, large writes are written directly
    if (write_size < WRITABLE_FILE_BUFFER_SIZE) {
      std::memcpy(buf, write_data, write_size);
      pos = write_size;
      return Status::ok();
    }

    return write_unbuffered(write_data, write_size);
  }

  Status close() override {
    Status status = flush_buffer();
    const int ret = ::close(fd);
    if (ret < 0 && status.is_ok()) {
      return posix_error(filename, errno);
    }
    fd = -1;
    return status;
  }

  Status flush() override {
    return flush_buffer();
  }

  Status sync() override {
    Status status = flush_buffer();
    if (!status.is_ok()) {
      return status;
    }

    return sync_fd(fd, filename);
  }

 private:
  Status flush_buffer() {
    Status status = write_unbuffered(buf, pos);
    pos = 0;
    return status;
  }

  Status write_unbuffered(const char *data, size_t size) {
    while (size > 0) {
      ssize_t write_result = ::write(fd, data, size);
      if (write_result < 0) {
        if (errno == EINTR) {
          continue;
        }
        return posix_error(filename, errno);
      }
      data += write_result;
      size -= write_result;
    }
    return Status::ok();
  }

  static Status sync_fd(int fd, const std::string &filename) {
    bool sync_success = ::fdatasync(fd) == 0;
    if (!sync_success) {
      return posix_error(filename, errno);
    }
    return Status::ok();
  }

  char buf[WRITABLE_FILE_BUFFER_SIZE];
  size_t pos;
  int fd;

  const std::string filename;
};

Status new_writable_file(const std::string &filename, 
    WritableFile **result) {
  int fd = ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644);
  if (fd < 0) {
    *result = nullptr;
    return posix_error(filename, errno);
  }
  
  *result = new PosixWritableFile(filename, fd);
  return Status::ok();
}

Status new_sequential_file(const std::string &filename,
    SequentialFile **result) {
  int fd = ::open(filename.c_str(), O_RDONLY);
  if (fd < 0) {
    *result = nullptr;
    return posix_error(filename, errno);
  }

  *result = new PosixSequentialFile(filename, fd);
  return Status::ok();
}

bool file_exists(const char *pathname) {
  return ::access(pathname, F_OK) == 0;
}

Status get_children(const std::string& directory_path,
    std::vector<std::string>* result) {
  result->clear();
  ::DIR *dir = ::opendir(directory_path.c_str());
  if (dir == nullptr) {
    return posix_error(directory_path, errno);
  }
  struct ::dirent *entry;
  while ((entry = ::readdir(dir)) != nullptr) {
    result->emplace_back(entry->d_name);
  }
  ::closedir(dir);
  return Status::ok();
}

Status create_directory(const std::string &dirname) {
  if (::mkdir(dirname.c_str(), 0755) != 0) {
    return posix_error(dirname, errno);
  }
  return Status::ok();
}

Status unlink(const std::string &pathname) {
  if (::unlink(pathname.c_str()) != 0) {
    return posix_error(pathname, errno);
  }
  return Status::ok();
}

} // namespace morph
