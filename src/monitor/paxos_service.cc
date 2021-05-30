#include "paxos_service.h"

#include <future>
#include <utility>

namespace morph {
namespace paxos {

PaxosService::PaxosService(const std::string &this_name, 
                           std::shared_ptr<MonitorCluster> monitor_cluster):
    this_name(this_name), running(true),
    monitor_cluster(monitor_cluster) {
  assert(monitor_cluster->size() % 2 == 1);  // Need odd number of paxos instance

  logger = init_logger(this_name);
  assert(logger != nullptr);

  paxos = std::make_unique<Paxos>(this_name);

  heartbeat_thread = std::make_unique<std::thread>(
    &PaxosService::broadcast_heartbeat_routine, this);
}

PaxosService::~PaxosService() {
  running = false;
  heartbeat_thread->join();
}

void PaxosService::accept_handler(const uint32_t log_index,
                                  const uint64_t proposal, 
                                  const std::string &value,
                                  uint64_t *min_proposal) {
  paxos->accept_handler(log_index, proposal, value, min_proposal);
}

void PaxosService::prepare_handler(const uint32_t log_index, 
                                   const uint64_t proposal, 
                                   uint64_t *out_accepted_proposal, 
                                   std::string *accepted_value) {
  paxos->prepare_handler(log_index, proposal, out_accepted_proposal, 
                         accepted_value);
}

void PaxosService::commit_handler(const uint32_t log_index,
                                  const uint64_t proposal) {
  paxos->commit_handler(log_index, proposal);
}


bool PaxosService::run(const std::string &value, uint64_t *log_index) {
  Log *log = paxos->get_unchosen_log();
  uint64_t proposal = paxos->choose_new_proposal_number(log);

  if (log->state == VALUE_ACCEPTED) {
    assert(false && "NOT YET IMPLEMENTED");
  }

  log->set_accepted(proposal, value);

  // Phase-1
  std::unique_ptr<PvPair> pair = broadcast_prepare(log->get_log_index(), proposal);

  // Phase-2
  const std::string *value_to_send = nullptr;
  if (pair == nullptr) {
    value_to_send = &value;
  } else {
    log->set_accepted(pair->accepted_proposal, pair->accepted_value);
    value_to_send = &pair->accepted_value;
  }

  bool chosen = broadcast_accept(log->get_log_index(), 
                                 proposal, *value_to_send);
  if (chosen) {
    log->set_chosen();
    *log_index = log->get_log_index();
  } else {
    log->reset();
  }

  broadcast_commit(log->get_log_index(), proposal);
  return chosen;
}

std::unique_ptr<PvPair> PaxosService::broadcast_prepare(
                                           const uint32_t log_index,
                                           const uint64_t proposal) {
  std::atomic<int> response_count(0);
  auto &cluster_map = monitor_cluster->cluster_map;
  const int target_count = cluster_map.size() / 2;

  assert(cluster_map.size() % 2 != 0);

  // Right now the membership is not implemented.
  std::list<std::future<std::unique_ptr<PvPair>>> futures;
  for (auto p = cluster_map.begin(); p != cluster_map.end(); ++p) {
    auto instance = p->second;
    if (instance->info.name == this_name) {
      continue;
    }

    auto f = std::async(std::launch::async,
      [this, instance, log_index, proposal, &response_count, target_count] {
        return send_prepare(instance, log_index, proposal, &response_count, 
                            target_count);
      });
    futures.push_back(std::move(f));
  }

  std::unique_ptr<PvPair> highest_pair = nullptr;
  for (auto &f: futures) {
    f.wait();
    assert(f.valid());
    auto t = f.get();
    if (t != nullptr && 
        (highest_pair == nullptr || 
         highest_pair->accepted_proposal < t->accepted_proposal)) {
      highest_pair = std::move(t);
    }
  }

  return highest_pair;
}

bool PaxosService::broadcast_accept(const uint32_t log_index,
                                    const uint64_t proposal, 
                                    const std::string &value) {
  std::atomic<int> response_count(0);
  auto &cluster_map = monitor_cluster->cluster_map;
  const int target_count = cluster_map.size() / 2;

  assert(cluster_map.size() % 2 != 0);

  // Right now the membership is not implemented.
  std::list<std::future<std::unique_ptr<uint64_t>>> futures;
  for (auto p = cluster_map.begin(); p != cluster_map.end(); ++p) {
    auto instance = p->second;
    if (instance->info.name == this_name) {
      continue;
    }

    auto x = [this, instance, log_index, proposal, &value, 
              &response_count, target_count] {
      return send_accept(instance, log_index, proposal, value, 
                         &response_count, target_count);
    };
    auto f = std::async(std::launch::async, x);
    futures.push_back(std::move(f));
  }

  std::vector<uint64_t> results;
  for (auto &f: futures) {
    f.wait();
    assert(f.valid());
    auto res = f.get();
    if (res != nullptr) {
      results.push_back(*res);
    }
  }

  for (uint64_t result: results) {
    if (result > proposal) {
      return false;
    }
  }

  return true;
}


void PaxosService::broadcast_commit(const uint32_t log_index,
                                    const uint64_t proposal) {
  auto &cluster_map = monitor_cluster->cluster_map;

  std::list<std::future<void>> futures;
  for (auto p = cluster_map.begin(); p != cluster_map.end(); ++p) {
    auto instance = p->second;
    if (instance->info.name == this_name) {
      continue;
    }

    auto x = [this, instance, log_index, proposal] {
      return send_commit(instance, log_index, proposal);
    };
    auto f = std::async(std::launch::async, x);
    futures.push_back(std::move(f));
  }

  for (auto &future: futures) {
    future.wait();
  }
}

void PaxosService::broadcast_heartbeat_routine() {
  // TODO: implement this
}

std::unique_ptr<PvPair> PaxosService::send_prepare(
          std::shared_ptr<MonitorInstance> instance, const uint32_t log_index,
          const uint64_t proposal, std::atomic<int> *response_count, 
          const int target_count) {
  while (response_count->load() < target_count) {
    grpc::ClientContext ctx;
    monitor_rpc::PrepareRequest request;
    monitor_rpc::PrepareReply reply;

    std::chrono::system_clock::time_point deadline = 
      std::chrono::system_clock::now() + std::chrono::milliseconds(100);
    ctx.set_deadline(deadline);
    request.set_proposal(proposal);
    request.set_log_index(log_index);
    request.set_proposer(this_name);

    logger->info(fmt::sprintf("send_prepare to [%s]: log_index[%d] proposal[%lu] target_count[%d] \n",
      instance->info.name.c_str(), log_index, proposal, target_count));

    auto s = instance->stub->prepare(&ctx, request, &reply);
    if (!s.ok()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    ++(*response_count);
    if (reply.ret_val() != S_NOT_LEADER) {
      return nullptr;
    }
    assert(reply.ret_val() == S_SUCCESS);
    uint64_t v = reply.accepted_proposal();
    const std::string &val = reply.accepted_value();
    return std::make_unique<PvPair>(v, val);
  }

  return nullptr;
}

std::unique_ptr<uint64_t> PaxosService::send_accept(
           std::shared_ptr<MonitorInstance> instance, 
           const uint32_t log_index, const uint64_t proposal,
           const std::string &value, std::atomic<int> *response_count, 
           const int target_count) {
  while (response_count->load() < target_count) {
    grpc::ClientContext ctx;
    monitor_rpc::AcceptRequest request;
    monitor_rpc::AcceptReply reply;

    std::chrono::system_clock::time_point deadline = 
      std::chrono::system_clock::now() + std::chrono::milliseconds(100);
    ctx.set_deadline(deadline);
    request.set_log_index(log_index);
    request.set_proposal(proposal);
    request.set_value(value);
    request.set_proposer(this_name);

    auto s = instance->stub->accept(&ctx, request, &reply);
    if (!s.ok()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    ++(*response_count);
    if (reply.ret_val() != S_NOT_LEADER) {
      return nullptr;
    }
    assert(reply.ret_val() == S_SUCCESS);

    return std::make_unique<uint64_t>(reply.min_proposal());
  }

  return nullptr;
}

void PaxosService::send_commit(std::shared_ptr<MonitorInstance> instance, 
                               const uint32_t log_index,
                               const uint64_t proposal) {
  grpc::ClientContext ctx;
  monitor_rpc::CommitRequest request;
  monitor_rpc::CommitReply reply;

  logger->info(fmt::sprintf(
    "send commit index[%d] proposal[%lu] to monitor[%s]\n",
    log_index, proposal, instance->info.name
  ));

  std::chrono::system_clock::time_point deadline = 
    std::chrono::system_clock::now() + std::chrono::milliseconds(100);
  ctx.set_deadline(deadline);
  request.set_log_index(log_index);
  request.set_proposal(proposal);

  auto s = instance->stub->commit(&ctx, request, &reply);
  if (!s.ok()) {
    // TODO: ?
    return;
  }
}

bool PaxosService::is_leader(const std::string &name) {
  std::vector<std::string> names;
  for (auto x = monitor_cluster->cluster_map.begin(); 
       x != monitor_cluster->cluster_map.end();
       ++x) {
    names.push_back(x->second->info.name);
  }
  sort(names.begin(), names.end());
  return names.back() == name;
}

void PaxosService::get_last_chosen_log(uint32_t *log_index, std::string *value) {
  paxos->get_last_chosen_log(log_index, value);
}

} // namespace paxos
} // namespace morph