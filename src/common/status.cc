// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "status.h"

#include <stdio.h>

namespace morph {

const char * Status::CopyState(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  char* result = new char[size + 5];
  memcpy(result, state, size + 5);
  return result;
}

Status::Status(Code code, const Slice& msg, const Slice& msg2) {
  assert(code != OK);
  const uint32_t len1 = msg.size();
  const uint32_t len2 = msg2.size();
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
  char* result = new char[size + 5];
  memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  memcpy(result + 5, msg.data(), len1);
  if (len2) {
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    memcpy(result + 7 + len1, msg2.data(), len2);
  }
  state = result;
}

std::string Status::to_string() const {
  if (state == nullptr) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case OK:
        type = "OK";
        break;
      case NOT_FOUND:
        type = "NotFound: ";
        break;
      case CORRUPTION:
        type = "Corruption: ";
        break;
      case NOT_SUPPORTED:
        type = "Not implemented: ";
        break;
      case INVALID_ARGUMENT:
        type = "Invalid argument: ";
        break;
      case IO_ERROR:
        type = "IO error: ";
        break;
      default:
        snprintf(tmp, sizeof(tmp),
                 "Unknown code(%d): ", static_cast<int>(code()));
        type = tmp;
        break;
    }
    std::string result(type);
    uint32_t length;
    memcpy(&length, state, sizeof(length));
    result.append(state + 5, length);
    return result;
  }
}

} // namespace morph 