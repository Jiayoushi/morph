#ifndef MORPH_TEST_INTEGRATION_CLUSTER_H
#define MORPH_TEST_INTEGRATION_CLUSTER_H

#include <grpcpp/grpcpp.h>

#include "monitor/monitor.h"
#include "os/oss.h"
#include "mds/mds.h"
#include "monitor/config.h"
#include "common/consistent_hash.h"
#include "tests/utils.h"

namespace morph {
namespace test {

class Cluster {
 public:
  using Mds = mds::MetadataServer;
  using Oss = os::ObjectStoreServer;
  using Monitor = monitor::Monitor;
  using MdsStub = mds_rpc::MetadataService::Stub;
  using MonitorStub = monitor_rpc::MonitorService::Stub;
  using OssStub = oss_rpc::ObjectStoreService::Stub;

  static const int MDS_START_PORT = 4000;
  static const int MONITOR_START_PORT = 5000;
  static const int OSS_START_PORT = 6000;

  Cluster(const int monitor_count, const int oss_count) {
    assert(monitor_count < 1000 && oss_count < 1000);

    mons.reserve(monitor_count);
    oss.reserve(oss_count);

    for (int i = 0; i < monitor_count; ++i) {
      auto mon_name = "mon" + std::to_string(i);
      auto mon_addr = "0.0.0.0:" + std::to_string(MONITOR_START_PORT + i);

      mons.push_back(std::make_shared<Monitor>(mon_name, mon_addr));
      mon_stubs.push_back(monitor_rpc::MonitorService::NewStub(
        CreateChannel(mon_addr, grpc::InsecureChannelCredentials())));
      monitor_config.infos.emplace_back(mon_name, mon_addr);
    }

    for (int i = 0; i < oss_count; ++i) {
      auto oss_name = "oss" + std::to_string(i);
      auto oss_addr = "0.0.0.0:" + std::to_string(OSS_START_PORT + i);

      oss.push_back(std::make_shared<Oss>(oss_name, oss_addr, monitor_config));
      oss_stubs.push_back(oss_rpc::ObjectStoreService::NewStub(
        CreateChannel(oss_addr, grpc::InsecureChannelCredentials())));
    }

    auto mds_addr = "0.0.0.0:" + std::to_string(MDS_START_PORT);
    mds = std::make_shared<Mds>("mds" + std::to_string(0), mds_addr, monitor_config);
    mds_stub = mds_rpc::MetadataService::NewStub(
      CreateChannel(mds_addr, grpc::InsecureChannelCredentials()));
  }

  ~Cluster()=default;

  void cleanup() {
    for (int i = 0; i < mons.size(); ++i) {
      delete_directory("mon" + std::to_string(i));
    }
    for (int i = 0; i < oss.size(); ++i) {
      delete_directory("oss" + std::to_string(i));
    }
    delete_directory("mds0");
  }

  void restart_mds() {
    mds = nullptr;
    mds_stub = nullptr;

    auto mds_addr = "0.0.0.0:" + std::to_string(MDS_START_PORT);
    mds = std::make_shared<Mds>("mds" + std::to_string(0), mds_addr, monitor_config);
    mds_stub = mds_rpc::MetadataService::NewStub(
      CreateChannel(mds_addr, grpc::InsecureChannelCredentials()));
  }

  monitor::Config monitor_config;

  std::shared_ptr<Mds> mds;
  std::vector<std::shared_ptr<Monitor>> mons;
  std::vector<std::shared_ptr<Oss>> oss;

  std::shared_ptr<MdsStub> mds_stub;
  std::vector<std::shared_ptr<MonitorStub>> mon_stubs;
  std::vector<std::shared_ptr<OssStub>> oss_stubs;
};

} // namespace test
} // namespace morph

#endif