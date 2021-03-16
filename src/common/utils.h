#ifndef MORPH_COMMON_UTILS_H
#define MORPH_COMMON_UTILS_H

#include <iostream>
#include <string>
#include <rpc/msgpack.hpp>
#include <bitset>
#include <atomic>

namespace morph {

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


}


#endif