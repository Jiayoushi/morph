#ifndef MORPH_MONITOR_CLUSTER_MANAGER_H
#define MORPH_MONITOR_CLUSTER_MANAGER_H

#include <memory>
#include <grpcpp/grpcpp.h>
#include <proto_out/monitor.grpc.pb.h>
#include <proto_out/oss.grpc.pb.h>
#include <proto_out/mds.grpc.pb.h>

#include "common/cluster.h"

namespace morph {

using MonitorService = monitor_rpc::MonitorService;
using MonitorCluster = Cluster<MonitorService>;
using MonitorInstance = MonitorCluster::ServiceInstance;
using InstanceNames = std::vector<std::string>;
using OssCluster = Cluster<oss_rpc::ObjectStoreService>;

// This class is needed to provide the synchronization so that 
// monitor can serve get requests while ask paxos to apply the changes.
// TODO: it should also allow to merge incoming requests while paxos is serving
//       previous run
// TODO: better use interface rather than template
template <typename ClusterType>
class ClusterManager {
 public:
  ClusterManager():
    cur_names(nullptr),
    cur_serialized(nullptr),
    cur_version(0),
    current(nullptr),
    next(nullptr) {

    current = std::make_unique<ClusterType>(0);
    next = std::make_unique<ClusterType>(0);
    update_data(0, nullptr);
  }

  void add_current(std::unique_ptr<ClusterType> cluster) {
    std::lock_guard<std::mutex> lock(mutex);
    current = std::move(cluster);
  }

  // Change the current cluster to the next one. Called after paxos saves the log.
  void update(const uint64_t version) {
    std::lock_guard<std::mutex> lock(mutex);
    current = next;
    next = std::make_shared<ClusterType>(*current);
    update_data(version, nullptr);
  }

  std::shared_ptr<InstanceNames> get_names() const {
    std::lock_guard<std::mutex> lock(mutex);
    return cur_names;
  }

  std::shared_ptr<ClusterType> get_current(
                                uint64_t *version, 
                                std::shared_ptr<std::string> *serialized) const {
    std::lock_guard<std::mutex> lock(mutex);
    if (version != nullptr) {
      *version = cur_version;
    }
    if (serialized != nullptr) {
      *serialized = cur_serialized;
    }
    return current;
  }

  // Add instance to the *next* cluster, and return the serialized
  // value upon success
  ClusterErrorCode add_instance(const Info &info, std::string *serialized) {
    std::lock_guard<std::mutex> lock(mutex);

    ClusterErrorCode result = next->add_instance(info);
    assert(result == S_SUCCESS);
    if (serialized) {
      *serialized = std::move(next->serialize());
    }
    return result;
  }

  ClusterErrorCode remove_instance(const Info &info, std::string *serialized) {
    std::lock_guard<std::mutex> lock(mutex);

    ClusterErrorCode result = next->remove_instance(info);
    assert(result == S_SUCCESS);
    if (serialized) {
      *serialized = std::move(next->serialize());
    }
    return result;
  }

  void update_to(const uint64_t version, const std::string &value) {
    std::unique_ptr<ClusterType> new_cluster = std::make_unique<ClusterType>();
    new_cluster->update_cluster(value);

    std::lock_guard<std::mutex> lock(mutex);

    current = std::move(new_cluster);
    next = std::make_unique<ClusterType>(*current);
    update_data(version, &value);
  }

 private:
  void update_data(const uint64_t version, const std::string *serialized) {
    assert(current != nullptr);
    cur_version = version;

    if (serialized) {
      cur_serialized = std::make_shared<std::string>(*serialized);
    } else {
      cur_serialized = std::make_shared<std::string>(std::move(current->serialize()));
    }

    std::vector<Info> infos = current->get_cluster_infos();
    cur_names = std::make_shared<InstanceNames>();
    for (const Info &info: infos) {
      cur_names->push_back(info.name);
    }
  }

  mutable std::mutex mutex;

  std::shared_ptr<InstanceNames> cur_names;
  std::shared_ptr<std::string> cur_serialized;
  uint64_t cur_version;
  
  std::shared_ptr<ClusterType> current;
  std::shared_ptr<ClusterType> next;
};

} // namespace morph

#endif