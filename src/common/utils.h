#ifndef MORPH_UTILS_H
#define MORPH_UTILS_H

#include <iostream>
#include <string>
#include <rpc/msgpack.hpp>
#include <bitset>
#include <atomic>
#include <spdlog/fmt/bundled/printf.h>

#include "status.h"
#include "env.h"
#include "coding.h"

namespace morph {

inline Status posix_error(const std::string &context, int error_number) {
  if (error_number == ENOENT) {
    return Status::not_found(context, std::strerror(error_number));
  } else {
    return Status::io_error(context, std::strerror(error_number));
  }
}

template <typename T>
std::string serialize(const T &object) {
  std::stringstream ss;
  clmdep_msgpack::pack(ss, object);
  return ss.str();
}

template <typename T>
void deserialize(const std::string &deserialized, T &object) {
  clmdep_msgpack::object_handle oh = clmdep_msgpack::unpack(deserialized.data(), deserialized.size());
  clmdep_msgpack::object obj = oh.get();
  obj.convert(object);
}


template <unsigned int T>
class Flags {
 public:
  std::atomic<std::bitset<T>> bits;

  Flags():
    bits() {}

  Flags(const Flags<T> &src) {
    bits.store(src.bits.load());
  }

  Flags<T> & operator=(const Flags<T> &src) {
    bits.store(src.bits.load());
    return *this;
  }

  void mark(uint32_t fg) {
    bits.store(bits.load().set(fg));
  }

  void unmark(uint32_t fg) {
    bits.store(bits.load().reset(fg));
  }

  bool marked(uint32_t fg) {
    return bits.load()[fg] == 1;
  }

  void reset() {
    bits.store(bits.load().reset());
  }
};

template <typename T>
void flag_mark(const std::shared_ptr<T> &t, uint32_t fg) {
  t->flags.bits.store(t->flags.bits.load().set(fg));
}

template <typename T>
void flag_unmark(const std::shared_ptr<T> &t, uint32_t fg) {
  t->flags.bits.store(t->flags.bits.load().reset(fg));
}

template <typename T>
bool flag_marked(const std::shared_ptr<T> &t, uint32_t fg) {
  return t->flags.bits.load()[fg] == 1;
}

template <typename T>
bool flag_marked(const T *t, uint32_t fg) {
  return t->flags.bits.load()[fg] == 1;
}

template <typename T>
void flag_mark(T *t, uint32_t fg) {
  t->flags.bits.store(t->flags.bits.load().set(fg));
}

template <typename T>
void flag_unmark(T *t, uint32_t fg) {
  t->flags.bits.store(t->flags.bits.load().reset(fg));
}

inline bool get_length_prefixed_slice(Slice *input, Slice *result) {
  uint32_t len;
  if (get_varint32(input, &len) && input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

inline void put_length_prefixed_slice(std::string *dst, const Slice &value) {
  put_varint32(dst, value.size());
  dst->append(value.data(), value.size());
}

// Format uint64_t number to a string of two uint32_t
inline std::string uint64_two(uint64_t v) {
  return fmt::sprintf("%lu|%lu", 
              v >> 32, 
              v & std::numeric_limits<uint32_t>::max());
}

} // namespace morph

#endif
