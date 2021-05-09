#ifndef MORPH_COMMON_CLUSTER_H
#define MORPH_COMMON_CLUSTER_H

#include <grpcpp/grpcpp.h>

#include "network.h"

namespace morph {

static const int INITIAL_CLUSTER_VERSION = 0;

enum ErrorCode {
  S_SUCCESS = 0,

  // Requester's version is bigger than this monitor's version
  S_VERSION_INVALID = 1,

  // Target server is not in the current cluster map
  S_NOT_FOUND = 2,

  // Target server already in the current cluster map
  S_EXISTS = 3,

  // Monitor failed to build network connection to the target server
  S_CONNECTION_FAILED = 4,

  // The format of server network address is not correct
  S_ADDR_INVALID = 5
};

template <typename Service>
class Cluster {
 public:
  using StubType = typename Service::Stub;

  Cluster():
    version(INITIAL_CLUSTER_VERSION) {}

  ~Cluster() {
    for (auto p = stub_map.begin(); p != stub_map.end(); ++p) {
      delete p->second;
    }
  }

  // Simply add the instance without establishing the connection
  ErrorCode add_instance(const NetworkAddress &addr) {
    if (stub_map.find(addr) != stub_map.end()) {
      return S_EXISTS;
    }
    stub_map.emplace(addr, nullptr);
    return S_SUCCESS;
  }

  ErrorCode remove_instance(const NetworkAddress &addr) {
    if (stub_map.find(addr) == stub_map.end()) {
      return S_NOT_FOUND;
    }
    stub_map.erase(addr);
    return S_SUCCESS;
  }
 
  // Establish connection
  // On success, return 0.
  // On error, return -1.
  ErrorCode connect_to(const NetworkAddress &addr);

  StubType * get_service(const NetworkAddress &addr) {
    auto p = stub_map.find(addr);
    assert(p != stub_map.end());
    return p->second;
  }

  void add_version_by_one() {
    ++version;
  }

  void set_version(uint64_t target) {
    assert(target > version);
    version = target;
  }

  uint64_t get_version() const {
    return version;
  }

  std::vector<NetworkAddress> get_cluster_addrs() const;

  // TODO: 
  template <typename ClusterMap>
  void update_stub_map(const ClusterMap &map) {}

 private:
  uint64_t version;
  std::unordered_map<NetworkAddress, StubType *> stub_map;
};


template <typename Service>
ErrorCode Cluster<Service>::connect_to(const NetworkAddress &addr) {
  using grpc::InsecureChannelCredentials;

  auto instance = stub_map.find(addr);
  if (instance == stub_map.end()) {
    return S_NOT_FOUND;
  }

  std::shared_ptr<grpc::Channel> ch = 
    grpc::CreateChannel(addr, InsecureChannelCredentials());
  instance->second = Service::NewStub(ch);

  return S_SUCCESS;
}

template <typename Service>
std::vector<NetworkAddress> Cluster<Service>::get_cluster_addrs() const {
  std::vector<NetworkAddress> result;
  for (auto p = stub_map.cbegin(); p != stub_map.cend(); ++p) {
    result.push_back(p->first);
  }
  return result;
}

} // namespace morph

#endif