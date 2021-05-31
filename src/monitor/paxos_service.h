#ifndef MORPH_PAXOS_PAXOS_SERVICE_H
#define MORPH_PAXOS_PAXOS_SERVICE_H

#include <grpcpp/grpcpp.h>
#include <proto_out/monitor.grpc.pb.h>

#include "paxos.h"
#include "common/logger.h"
#include "common/cluster.h"
#include "common/config.h"

namespace morph {
namespace paxos {

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using MonitorService = monitor_rpc::MonitorService;
using MonitorCluster = Cluster<MonitorService>;
using MonitorInstance = MonitorCluster::ServiceInstance;
using ProposalValuePair = std::pair<uint64_t, std::string>;

struct PvPair {
  uint64_t accepted_proposal;
  std::string accepted_value;

  PvPair(const uint64_t accepted_proposal,
         const std::string accepted_value):
    accepted_proposal(accepted_proposal),
    accepted_value(accepted_value) {}
};

class PaxosService {
 public:
  PaxosService(const std::string &this_name, 
               std::shared_ptr<MonitorCluster> monitor_cluster);

  ~PaxosService();

  // Returns whehter the value is chosen
  bool run(const std::string &value, uint64_t *log_index);

  void prepare_handler(const uint32_t log_index, const uint64_t proposal, 
                       uint64_t *out_accepted_proposal,
                       std::string *accepted_value);

  void accept_handler(const uint32_t log_index,
                      const uint64_t proposal, 
                      const std::string &value,
                      uint64_t *min_proposal);

  void commit_handler(const uint32_t log_index,
                      const uint64_t proposal);

  bool is_leader(const std::string &name);

  std::shared_ptr<MonitorInstance> get_leader();

  void get_last_chosen_log(uint32_t *log_index, std::string *value);

 private:
  std::unique_ptr<PvPair> broadcast_prepare(const uint32_t log_index, 
                                            const uint64_t proposal);

  bool broadcast_accept(const uint32_t log_index, const uint64_t proposal, 
                        const std::string &value);

  void broadcast_commit(const uint32_t log_index,
                        const uint64_t proposal);

  void broadcast_heartbeat_routine();

  // Returns <accepted_proposal, accepted_value>
  // Returns nullptr if the receiver does not think you are the leader
  //                 or timeout before reciever returned anything
  std::unique_ptr<PvPair> send_prepare(
                             std::shared_ptr<MonitorInstance> instance, 
                             const uint32_t log_index,
                             const uint64_t proposal,
                             std::atomic<int> *response_count,
                             const int target_count);

  // Return min_proposal
  // Returns nullptr if the receiver does not think you are the leader
  //                 or timeout before reciever returned anything
  std::unique_ptr<uint64_t> send_accept(std::shared_ptr<MonitorInstance> instance, 
                                        const uint32_t log_index,
                                        const uint64_t proposal,
                                        const std::string &value,
                                        std::atomic<int> *response_count,
                                        const int target_count);

  void send_commit(std::shared_ptr<MonitorInstance> instance, 
                   const uint32_t log_index,
                   const uint64_t proposal);

  const std::string this_name;

  std::mutex mutex;

  std::atomic<bool> running;

  std::shared_ptr<spdlog::logger> logger;

  std::unique_ptr<std::thread> heartbeat_thread;

  std::unique_ptr<Paxos> paxos;

  std::shared_ptr<MonitorCluster> monitor_cluster;
};


} // namespace paxos
} // namespace morph

#endif