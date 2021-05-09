// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Code modified.

#ifndef MORPH_ENV_H
#define MORPH_ENV_H

#include <memory>
#include <vector>

#include "status.h"
#include "nocopy.h"

namespace morph {

// A file abstraction for sequential writing. The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class WritableFile: NoCopy {
 public:
  WritableFile() = default;

  virtual ~WritableFile();

  virtual Status append(const Slice &data) = 0;

  virtual Status close() = 0;

  virtual Status flush() = 0;

  virtual Status sync() = 0;
};

class SequentialFile: NoCopy {
 public:
  SequentialFile() = default;

  virtual ~SequentialFile();

  virtual Status read(size_t n, Slice *result, char *scratch) = 0;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  // 
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  // 
  // REQUIRES: External synchronization
  virtual Status skip(uint64_t n) = 0;
};

Status new_writable_file(const std::string &filename, 
  WritableFile **result);

Status new_sequential_file(const std::string &filename,
  SequentialFile **result);

bool file_exists(const char *pathname);

Status get_children(const std::string& directory_path,
    std::vector<std::string> *result);

Status create_directory(const std::string &name);

} // namespace morph

#endif
