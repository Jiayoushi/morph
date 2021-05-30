#ifndef MORPH_COMMON_CLUSTER_H
#define MORPH_COMMON_CLUSTER_H

#include <grpcpp/grpcpp.h>

#include "network.h"
#include "config.h"

namespace morph {

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
  S_ADDR_INVALID = 5,

  S_NOT_LEADER = 6
};

// No lock needed. The synchronization of cluster change is provided by 
// ClusterManager.
// TODO(OPTIMIZATION): it's probably better to write a 
//                     service interface and then that other
//                     implement this interface
// 
template <typename Service>
class Cluster {
 public:
  using StubType = typename Service::Stub;

  struct ServiceInstance {
    Info info;
    StubType *stub;

    ServiceInstance() = delete;

    ServiceInstance(const Info &info):
        info(info), stub(nullptr) {
      std::shared_ptr<grpc::Channel> ch = 
          grpc::CreateChannel(info.addr, grpc::InsecureChannelCredentials());
      assert(ch != nullptr);
      stub = Service::NewStub(ch).release();
      assert(stub != nullptr);
    }

    ServiceInstance & operator=(const ServiceInstance &x) {
      info = x.info;
      stub = x.stub;
    }

    bool operator==(const ServiceInstance &x) {
      return info == x.info;
    }

    bool operator!=(const ServiceInstance &x) {
      return info != x.info;
    }

    ~ServiceInstance() {
      delete stub;
    }

    MSGPACK_DEFINE_ARRAY(info);
  };

  Cluster() {}

  // Add all except for this_info
  Cluster(const Config &config) {
    for (const auto &info: config.infos) {
      assert(add_instance(info) == S_SUCCESS);
    }
  }

  Cluster & operator=(const Cluster &c) {
    this->cluster_map = c.cluster_map;
  }

  ClusterErrorCode add_instance(const Info &info) {
    if (!verify_network_address(info.addr)) {
      return S_ADDR_INVALID;
    }
    if (cluster_map.find(info.name) != cluster_map.end()) {
      return S_EXISTS;
    }
    cluster_map.emplace(info.name, std::make_shared<ServiceInstance>(info));
    return S_SUCCESS;
  }

  ClusterErrorCode remove_instance(const Info &info) {
    if (cluster_map.find(info.name) == cluster_map.end()) {
      return S_NOT_FOUND;
    }
    cluster_map.erase(info.name);
    return S_SUCCESS;
  }

  std::shared_ptr<ServiceInstance> get_instance(const std::string &name) {
    auto p = cluster_map.find(name);
    if (p == cluster_map.end()) {
      return nullptr;
    }
    return p->second;
  }

  size_t size() const {
    return cluster_map.size();
  }

  std::vector<Info> get_cluster_infos() const;

  void update_cluster(const std::string &s) {
    using namespace clmdep_msgpack;

    if (s.empty()) {
      return;
    }

    std::vector<Info> infos;
    object_handle oh = unpack(s.data(), s.size());
    object obj = oh.get();
    obj.convert(infos);

    update_cluster(infos);
  }

  std::string serialize() const {
    std::stringstream ss;
    std::vector<Info> infos;
    for (auto p = cluster_map.begin(); p != cluster_map.end(); ++p) {
      infos.emplace_back(p->second->info.name, p->second->info.addr);
    }
    clmdep_msgpack::pack(ss, infos);
    return ss.str();
  }

  bool operator==(const Cluster<Service> &x) const {
    if (cluster_map.size() != x.cluster_map.size()) {
      return false;
    }

    for (auto iter = cluster_map.begin(); iter != cluster_map.end(); ++iter) {
      auto res = x.cluster_map.find(iter->first);
      if (res == x.cluster_map.end()) {
        return false;
      }
      if (*res->second != *iter->second) {
        return false;
      }
    }
    return true;
  }


  std::unordered_map<std::string, std::shared_ptr<ServiceInstance>> cluster_map;

 private:
  void update_cluster(const std::vector<Info> &new_cluster) {
    for (const Info &info: new_cluster) {
      add_instance(info);
    }

    std::vector<std::string> to_delete;
    for (auto p = cluster_map.cbegin(); p != cluster_map.cend(); ++p) {
      bool found = false;
      for (int i = 0; i < new_cluster.size(); ++i) {
        if (new_cluster[i].name == p->second->info.name) {
          found = true;
          break;
        }
      }
      if (!found) {
        to_delete.push_back(p->second->info.name);
      }
    }

    for (const std::string &name: to_delete) {
      cluster_map.erase(name);
    }


  }
};

template <typename Service>
std::vector<Info> Cluster<Service>::get_cluster_infos() const {
  std::vector<Info> result;
  for (auto p = cluster_map.cbegin(); p != cluster_map.cend(); ++p) {
    result.emplace_back(p->first, p->second->info.addr);
  }
  return result;
}

} // namespace morph

#endif