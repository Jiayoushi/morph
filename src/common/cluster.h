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

  // TODO: move this outside since other code need to access it
  struct ServiceInstance {
    std::string name;
    NetworkAddress addr;
    StubType *stub;

    ServiceInstance() = delete;

    ServiceInstance(const std::string &name,
                    const NetworkAddress &addr):
        name(name), addr(addr), stub(nullptr) {
      std::shared_ptr<grpc::Channel> ch = 
          grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
      assert(ch != nullptr);
      stub = Service::NewStub(ch).release();
      assert(stub != nullptr);
    }

    ~ServiceInstance() {
      delete stub;
    }
  };

  Cluster():
    version(INITIAL_CLUSTER_VERSION),
    cluster_names(nullptr) {}

  // Simply add the instance without establishing the connection
  ClusterErrorCode add_instance(const std::string &name, 
                                const NetworkAddress &addr) {
    std::lock_guard<std::mutex> lock(mutex);

    if (!verify_network_address(addr)) {
      return S_ADDR_INVALID;
    }
    if (cluster_map.find(name) != cluster_map.end()) {
      return S_EXISTS;
    }
    cluster_map.emplace(name, std::make_shared<ServiceInstance>(name, addr));
    updated_cluster_names();
    ++version;
    return S_SUCCESS;
  }

  ClusterErrorCode remove_instance(const std::string &name) {
    std::lock_guard<std::mutex> lock(mutex);

    if (cluster_map.find(name) == cluster_map.end()) {
      return S_NOT_FOUND;
    }
    cluster_map.erase(name);
    updated_cluster_names();
    ++version;
    return S_SUCCESS;
  }

  std::shared_ptr<ServiceInstance> get_instance(const std::string &name) {
    std::lock_guard<std::mutex> lock(mutex);

    auto p = cluster_map.find(name);
    if (p == cluster_map.end()) {
      return nullptr;
    }
    return p->second;
  }

  void set_version(uint64_t target) {
    std::lock_guard<std::mutex> lock(mutex);
    assert(target > version);
    version = target;
  }

  uint64_t get_version() const {
    std::lock_guard<std::mutex> lock(mutex);
    return version;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex);
    return cluster_map.size();
  }

  std::shared_ptr<std::vector<std::string>> get_cluster_names() const;

  std::vector<Info> get_cluster_infos() const;

  void update_cluster(uint64_t ver, 
           const std::unordered_map<std::string, NetworkAddress> &new_cluster) {
    for (auto p = new_cluster.cbegin(); p != new_cluster.end(); ++p) {
      add_instance(p->first, p->second);
    }

    std::vector<std::string> to_delete;
    for (auto p = cluster_map.cbegin(); p != cluster_map.cend(); ++p) {
      if (new_cluster.find(p->second->name) == new_cluster.end()) {
        to_delete.push_back(p->second->name);
      }
    }

    for (const std::string &name: to_delete) {
      cluster_map.erase(name);
    }

    updated_cluster_names();
    version = ver;
  }

  void lock() {
    mutex.lock();
  }

  void unlock() {
    mutex.unlock();
  }

 private:
  void updated_cluster_names() {
    std::shared_ptr<std::vector<std::string>> result = 
      std::make_shared<std::vector<std::string>>();

    for (auto p = cluster_map.cbegin(); p != cluster_map.cend(); ++p) {
      result->push_back(p->second->name);
    }
    cluster_names = result;
  }


  mutable std::mutex mutex;

  uint64_t version;

  std::shared_ptr<std::vector<std::string>> cluster_names;

  std::unordered_map<std::string, std::shared_ptr<ServiceInstance>> cluster_map;
};

template <typename Service>
std::shared_ptr<std::vector<std::string>> Cluster<Service>::get_cluster_names() const {
  std::lock_guard<std::mutex> lock(mutex);
  return cluster_names;
}

template <typename Service>
std::vector<Info> Cluster<Service>::get_cluster_infos() const {
  std::lock_guard<std::mutex> lock(mutex);
  std::vector<Info> result;
  for (auto p = cluster_map.cbegin(); p != cluster_map.cend(); ++p) {
    result.emplace_back(p->first, p->second->addr);
  }
  return result;
}

} // namespace morph

#endif