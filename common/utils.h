#ifndef MORPH_COMMON_UTILS_H
#define MORPH_COMMON_UTILS_H

#include <iostream>
#include <string>
#include <rpc/msgpack.hpp>

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

}


#endif