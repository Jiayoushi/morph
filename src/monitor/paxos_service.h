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

const int HEARTBEAT_BROADCAST_INTERVAL = 200;
const int HEARTBEAT_TIMEOUT_INTERVAL = 800;

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

  // Return either success, failure or not_leader
  int run(const std::string &value, uint64_t *log_index);



  // Return either success or not_leader
  int prepare_handler(const std::string &proposer,
                      const uint32_t log_index, 
                      const uint64_t proposal, 
                      uint64_t *out_accepted_proposal,
                      std::string *accepted_value);

  // Return either success or not_leader
  int accept_handler(const std::string &proposer,
                     const uint32_t log_index,
                     const uint32_t first_unchosen,
                     const uint64_t proposal, 
                     const std::string &value,
                     uint64_t *min_proposal,
                     uint32_t *first_unchosen_index);

  void success_handler(const uint32_t log_index,
                       const std::string &value);

  bool is_leader(const std::string &name);

  std::shared_ptr<MonitorInstance> get_leader();

  void get_last_chosen_log(uint32_t *log_index, std::string *value);
  
  uint32_t get_first_unchosen_index() const;

  void get_serialized_logs(std::string *buf) const;

 private:
  // Responsible for replicating the logs to all acceptors
  struct SyncControlBlock {
    mutable std::mutex mutex;

    uint32_t first_unchosen_index;

    std::condition_variable state_changed;

    std::unique_ptr<std::thread> sync_thread;

    std::shared_ptr<MonitorInstance> instance;

    PaxosService *paxos_service;

    SyncControlBlock(std::shared_ptr<MonitorInstance> instance, PaxosService *paxos_service):
        first_unchosen_index(0),
        instance(instance),
        paxos_service(paxos_service) {
      sync_thread = std::make_unique<std::thread>(&PaxosService::sync_routine, paxos_service, this);
    }

    ~SyncControlBlock() {
      sync_thread->join();
    }

    void set_first_unchosen_index(const uint64_t new_first_unchosen_idx) {
      std::lock_guard<std::mutex> lock(mutex);
      if (new_first_unchosen_idx > first_unchosen_index) {
        first_unchosen_index = new_first_unchosen_idx;
        state_changed.notify_all();
      }
    }


    uint64_t get_first_unchosen_index() const {
      std::lock_guard<std::mutex> lock(mutex);
      return first_unchosen_index;
    }

    bool need_sync() {
      std::lock_guard<std::mutex> lock(mutex);
      return paxos_service->paxos->get_first_unchosen_index() > first_unchosen_index;
    }
  };

  // Return either success, failure or not_leader
  int run_one_round(const std::string &value, uint64_t *log_index);

  std::unique_ptr<PvPair> broadcast_prepare(const uint32_t log_index, 
                                            const uint64_t proposal);

  bool broadcast_accept(const uint32_t log_index, const uint32_t first_unchosen_index,
                        const uint64_t proposal, const std::string &value);

  // Returns <accepted_proposal, accepted_value>
  std::unique_ptr<PvPair> send_prepare(
                             std::shared_ptr<MonitorInstance> instance, 
                             const uint32_t log_index,
                             const uint64_t proposal,
                             std::atomic<int> *response_count,
                             const int target_count);

  // Return min_proposal
  std::unique_ptr<uint64_t> send_accept_until(
                                        std::shared_ptr<MonitorInstance> instance, 
                                        const uint32_t log_index,
                                        const uint32_t first_unchosen_index,
                                        const uint64_t proposal,
                                        const std::string &value,
                                        std::atomic<int> *response_count,
                                        const int target_count);

  void send_success(std::shared_ptr<MonitorInstance> instance, 
                    const uint32_t log_index,
                    const std::string &value);

  void sync_routine(PaxosService::SyncControlBlock *scb);

  void set_instance_first_unchosen_index(const std::string &name, const uint64_t first) {
    auto scb = sync_cbs.find(name);
    if (scb != sync_cbs.end()) {
      scb->second->set_first_unchosen_index(first);
    }
  }

  int get_instance_first_unchosen_index(const std::string &name, uint64_t *first) {
    auto scb = sync_cbs.find(name);
    if (scb == sync_cbs.end()) {
      logger->info(fmt::sprintf("failed to get sync control block for monitor [%s]",
        name.c_str()));
      return -1;
    }
    *first = scb->second->get_first_unchosen_index();
    return 0;
  }

  const std::string this_name;

  std::mutex mutex;

  std::atomic<bool> running;

  std::shared_ptr<spdlog::logger> logger;

  std::unique_ptr<Paxos> paxos;

  std::shared_ptr<MonitorCluster> monitor_cluster;

  std::unordered_map<std::string, SyncControlBlock *> sync_cbs;
};


} // namespace paxos
} // namespace morph

#endif