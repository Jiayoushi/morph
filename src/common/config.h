#ifndef MORPH_COMMON_CONFIG_H
#define MORPH_COMMON_CONFIG_H

#include <cassert>
#include <vector>
#include <string>
#include <rpc/msgpack.hpp>
#include <spdlog/fmt/bundled/printf.h>


#include "network.h"

namespace morph {

struct Info {
  std::string name;
  NetworkAddress addr;
  bool local;

  MSGPACK_DEFINE_ARRAY(name, addr);

  Info():
    name(), addr(), local(false) {}

  Info(const std::string &name, const NetworkAddress &addr):
      name(name), addr(addr), local(false) {
    assert(verify_network_address(addr));
  }

  Info(const Info &info) = default;
  Info & operator=(const Info &info) = default;

  bool operator==(const Info &x) const {
    return name == x.name && addr == x.addr;
  }

  bool operator!=(const Info &x) const {
    return name != x.name || addr != x.addr;
  }

  std::string to_string() const {
    return fmt::sprintf("Info(name[%s] addr[%s])",
      name.c_str(), addr.c_str());
  }
};

struct InfoHash {
  std::string operator() (const Info &info) const {
    return info.name + info.addr;
  }
};

struct Config {
  std::vector<Info> infos;
  Info *this_info;

  Config() = delete;
  Config(const int total):
      this_info(nullptr) {
    infos.reserve(total);
  }

  void add(const std::string &name, const NetworkAddress &addr) {
    assert(infos.size() < infos.capacity());
    infos.emplace_back(name, addr);
  }

  void set_this(size_t index) {
    assert(index < infos.size());
    assert(infos.size() == infos.capacity());

    for (int i = 0; i < infos.size(); ++i) {
      if (i == index) {
        infos[i].local = true;
      } else {
        infos[i].local = false;
      }
    }
    this_info = &infos[index];
  }
};

} // namespace morph


#endif