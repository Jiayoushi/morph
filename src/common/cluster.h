#ifndef MORPH_COMMON_CLUSTER_H
#define MORPH_COMMON_CLUSTER_H

#include <grpcpp/grpcpp.h>

#include "network.h"

namespace morph {

static const int INITIAL_CLUSTER_VERSION = 0;

enum ClusterErrorCode {
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

struct Info {
  std::string name;
  NetworkAddress addr;

  Info(const std::string &name, const NetworkAddress &addr):
    name(name), addr(addr) {}
};



template <typename Service>
class Cluster {
 public:
  using StubType = typename Service::Stub;

  struct ServiceInstance {
    std::string name;
    NetworkAddress addr;
    StubType *stub;

    ServiceInstance() = delete;

    ServiceInstance(const std::string &name,
                    const NetworkAddress &addr):
        name(name), addr(addr), stub(nullptr) {}

    ~ServiceInstance() {
      delete stub;
    }

    bool connected() const {
      return stub != nullptr;
    }

    void connect() {
      std::shared_ptr<grpc::Channel> ch = 
        grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
      assert(ch != nullptr);
      stub = Service::NewStub(ch).release();
    }
  };

  Cluster():
    version(INITIAL_CLUSTER_VERSION) {}

  // Simply add the instance without establishing the connection
  ClusterErrorCode add_instance(const std::string &name, 
                                const NetworkAddress &addr) {
    if (!verify_network_address(addr)) {
      return S_ADDR_INVALID;
    }
    if (cluster_map.find(name) != cluster_map.end()) {
      return S_EXISTS;
    }
    cluster_map.emplace(name, ServiceInstance(name, addr));
    return S_SUCCESS;
  }

  ClusterErrorCode remove_instance(const std::string &name) {
    if (cluster_map.find(name) == cluster_map.end()) {
      return S_NOT_FOUND;
    }
    cluster_map.erase(name);
    return S_SUCCESS;
  }

  ServiceInstance * get_instance(const std::string &name) {
    auto p = cluster_map.find(name);
    assert(p != cluster_map.end());
    return &p->second;
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

  size_t size() const {
    return cluster_map.size();
  }

  std::vector<std::string> get_cluster_names() const;

  std::vector<Info> get_cluster_infos() const;

  void update_cluster(uint64_t ver, 
           const std::unordered_map<std::string, NetworkAddress> &new_cluster) {
    for (auto p = new_cluster.cbegin(); p != new_cluster.end(); ++p) {
      add_instance(p->first, p->second);
    }

    std::vector<std::string> to_delete;
    for (auto p = cluster_map.cbegin(); p != cluster_map.cend(); ++p) {
      if (new_cluster.find(p->second.name) == new_cluster.end()) {
        to_delete.push_back(p->second.name);
      }
    }

    for (const std::string &name: to_delete) {
      cluster_map.erase(name);
    }

    version = ver;
  }


  // TODO: it's probably better to provide a iterator? But it's public for now,
  //       so that monitor service impl can broadcast cluster map without 
  //       coding the rpc parameters inside this class.
  std::unordered_map<std::string, ServiceInstance> cluster_map;

 private:
  uint64_t version;
};

template <typename Service>
std::vector<std::string> Cluster<Service>::get_cluster_names() const {
  std::vector<std::string> result;
  for (auto p = cluster_map.cbegin(); p != cluster_map.cend(); ++p) {
    result.push_back(p->second.name);
  }
  return result;
}


// TODO(optimization): kinda slow to generate a vector each time
template <typename Service>
std::vector<Info> Cluster<Service>::get_cluster_infos() const {
  std::vector<Info> result;
  for (auto p = cluster_map.cbegin(); p != cluster_map.cend(); ++p) {
    result.emplace_back(p->first, p->second.addr);
  }
  return result;
}

} // namespace morph

#endif