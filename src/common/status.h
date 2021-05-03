// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.
//
// Code modified.

#ifndef MORPH_STATUS_H
#define MORPH_STATUS_H

#include <algorithm>
#include <string>

#include <common/slice.h>

namespace morph {

class Status {
 public:
  // Create a success status.
  Status() noexcept:
    state(nullptr) {}

  ~Status() { 
    delete[] state; 
  }

  Status(const Status &rhs);

  Status & operator=(const Status &rhs);

  Status(Status &&rhs) noexcept:
      state(rhs.state) {
    rhs.state = nullptr; 
  }

  Status & operator=(Status&& rhs) noexcept;

  // Return a success status.
  static Status ok() {
    return Status(); 
  }

  static Status not_found(const Slice &msg, const Slice &msg2 = Slice()) {
    return Status(NOT_FOUND, msg, msg2);
  }

  static Status corruption(const Slice &msg, const Slice &msg2 = Slice()) {
    return Status(CORRUPTION, msg, msg2);
  }
  static Status not_supported(const Slice &msg, const Slice &msg2 = Slice()) {
    return Status(NOT_SUPPORTED, msg, msg2);
  }
  static Status invalid_argument(const Slice &msg, const Slice &msg2 = Slice()) {
    return Status(INVALID_ARGUMENT, msg, msg2);
  }
  static Status io_error(const Slice &msg, const Slice &msg2 = Slice()) {
    return Status(IO_ERROR, msg, msg2);
  }

  // Returns true iff the status indicates success.
  bool is_ok() const { 
    return state == nullptr; 
  }

  // Returns true iff the status indicates a NotFound error.
  bool is_not_found() const { 
    return code() == NOT_FOUND; 
  }

  // Returns true iff the status indicates a Corruption error.
  bool is_corruption() const { 
    return code() == CORRUPTION; 
  }

  // Returns true iff the status indicates an IOError.
  bool is_io_error() const { 
    return code() == IO_ERROR; 
  }

  // Returns true iff the status indicates a NotSupportedError.
  bool is_not_supported_error() const { 
    return code() == NOT_SUPPORTED; 
  }

  // Returns true iff the status indicates an InvalidArgument.
  bool is_invalid_argument() const { 
    return code() == INVALID_ARGUMENT; 
  }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string to_string() const;

 private:
  enum Code {
    OK = 0,
    NOT_FOUND = 1,
    CORRUPTION = 2,
    NOT_SUPPORTED = 3,
    INVALID_ARGUMENT = 4,
    IO_ERROR = 5
  };

  Code code() const {
    return (state == nullptr) ? OK : static_cast<Code>(state[4]);
  }

  Status(Code code, const Slice &msg, const Slice &msg2);

  static const char * CopyState(const char *s);

  // OK status has a null state.  Otherwise, state is a new[] array
  // of the following form:
  //    state[0..3] == length of message
  //    state[4]    == code
  //    state[5..]  == message
  const char* state;
};

inline Status::Status(const Status &rhs) {
  state = (rhs.state == nullptr) ? nullptr : CopyState(rhs.state);
}

inline Status & Status::operator=(const Status &rhs) {
  // The following condition catches both aliasing (when this == &rhs),
  // and the common case where both rhs and *this are ok.
  if (state != rhs.state) {
    delete[] state;
    state = (rhs.state == nullptr) ? nullptr : CopyState(rhs.state);
  }
  return *this;
}
inline Status & Status::operator=(Status &&rhs) noexcept {
  std::swap(state, rhs.state);
  return *this;
}

} // namespace morph

#endif
