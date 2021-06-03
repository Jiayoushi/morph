#include "paxos_service.h"

#include <future>
#include <utility>
#include "common/utils.h"

namespace morph {
namespace paxos {

PaxosService::PaxosService(const std::string &this_name, 
                           std::shared_ptr<MonitorCluster> monitor_cluster):
    this_name(this_name), running(true),
    monitor_cluster(monitor_cluster) {
  // Need odd number of paxos instance
  assert(monitor_cluster->size() % 2 == 1);

  logger = init_logger(this_name);
  assert(logger != nullptr);

  paxos = std::make_unique<Paxos>(this_name);

  sync_cbs.reserve(monitor_cluster->size() - 1);
  for (auto p = monitor_cluster->cluster_map.begin();
        p != monitor_cluster->cluster_map.end();
        ++p) {
    if (!p->second->is_local()) {
      sync_cbs[p->second->info.name] = new SyncControlBlock(p->second, this);
    }
  }

  logger->info(fmt::sprintf("PaxoseService constructor finished."));
}

PaxosService::~PaxosService() {
  running = false;

  for (auto p = sync_cbs.begin(); p != sync_cbs.end(); ++p) {
    delete p->second;
  }
}

int PaxosService::prepare_handler(const std::string &proposer,
                                  const uint32_t log_index, 
                                  const uint64_t proposal, 
                                  uint64_t *out_accepted_proposal, 
                                  std::string *accepted_value) {
  if (!is_leader(proposer)) {
    return S_NOT_LEADER;
  }
  paxos->prepare_handler(log_index, proposal, out_accepted_proposal, 
                         accepted_value);
  return S_SUCCESS;
}

int PaxosService::accept_handler(const std::string &proposer,
                                 const uint32_t log_index,
                                 const uint32_t first_unchosen,
                                 const uint64_t proposal,
                                 const std::string &value,
                                 uint64_t *min_proposal,
                                 uint32_t *first_unchosen_index) {
  set_instance_first_unchosen_index(proposer, first_unchosen);
  paxos->get_min_pro_and_first_unchosen(min_proposal, first_unchosen_index);
  if (!is_leader(proposer)) {
    return S_NOT_LEADER;
  }
  paxos->accept_handler(log_index, first_unchosen, proposal, value, 
                        min_proposal);
  return S_SUCCESS;
}

void PaxosService::success_handler(const uint32_t log_index,
                                   const std::string &value) {
  paxos->success_handler(log_index, value);
}

int PaxosService::run(const std::string &value, uint64_t *log_index) {
  int ret;
  for (int i = 0; i < 2; ++i) {
    ret = run_one_round(value, log_index);
    if (ret == S_SUCCESS) {
      return ret;
    }
  }
  return ret;
}

int PaxosService::run_one_round(const std::string &value, 
                                uint64_t *log_index) {
  if (!is_leader(this_name)) {
    return S_NOT_LEADER;
  }

  Log *log = paxos->get_unchosen_log();
  uint64_t proposal = paxos->choose_new_proposal_number();

  if (paxos->has_accepted_proposal(log)) {
    assert(false && "NOT YET IMPLEMENTED");
  }

  // Phase-1
  std::unique_ptr<PvPair> pair;
  pair = broadcast_prepare(paxos->get_log_index(log), 
                           proposal);
  if (!is_leader(this_name)) {
    return S_NOT_LEADER;
  }

  // Phase-2
  const std::string *value_to_accept = nullptr;
  if (pair->accepted_proposal == 0) {
    value_to_accept = &value;
  } else {
    value_to_accept = &pair->accepted_value;
  }
  paxos->set_accepted(log, proposal, *value_to_accept);

  bool chosen = broadcast_accept(paxos->get_log_index(log), 
                                 paxos->get_first_unchosen_index(),
                                 proposal, *value_to_accept);
  if (chosen) {
    paxos->set_chosen(log, nullptr);
    *log_index = paxos->get_log_index(log);
    return S_SUCCESS;
  } else {
    paxos->reset(log);
    return S_FAILURE;
  }
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
    if (instance->is_local()) {
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
    if (highest_pair == nullptr || 
        highest_pair->accepted_proposal < t->accepted_proposal) {
      highest_pair = std::move(t);
    }
  }

  return highest_pair;
}

bool PaxosService::broadcast_accept(const uint32_t log_index,
                                    const uint32_t first_unchosen_index,
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

    auto x = [this, instance, log_index, first_unchosen_index, proposal, 
              &value, &response_count, target_count] {
      return send_accept_until(instance, log_index, first_unchosen_index,
                         proposal, value, &response_count, target_count);
    };
    auto f = std::async(std::launch::async, x);
    futures.push_back(std::move(f));
  }

  std::vector<uint64_t> results;
  for (auto &f: futures) {
    f.wait();
    assert(f.valid());
    auto res = f.get();
    results.push_back(*res);
  }

  for (uint64_t result: results) {
    if (result > proposal) {
      return false;
    }
  }

  return true;
}

std::unique_ptr<PvPair> PaxosService::send_prepare(
          std::shared_ptr<MonitorInstance> instance, const uint32_t log_index,
          const uint64_t proposal, std::atomic<int> *response_count, 
          const int target_count) {
  monitor_rpc::PrepareRequest request;
  request.set_proposal(proposal);
  request.set_log_index(log_index);
  request.set_proposer(this_name);

  while (response_count->load() < target_count) {
    grpc::ClientContext ctx;
    monitor_rpc::PrepareReply reply;

    std::chrono::system_clock::time_point deadline = 
      std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
    ctx.set_deadline(deadline);

    logger->info(fmt::sprintf(
      "send_prepare to [%s]: log_index[%d] proposal[%s] target_count[%d]",
      instance->info.name.c_str(), log_index, uint64_two(proposal), target_count));

    grpc::Status s = instance->stub->prepare(&ctx, request, &reply);

    logger->info(fmt::sprintf(
      "send_prepare [%s]: log_index[%d] proposal[%s] target_count[%d]."
      " got result: ac_proposal[%s] ac_value[%s] rpc[%d] ret[%d]",
      instance->info.name.c_str(), log_index, uint64_two(proposal), target_count,
      uint64_two(reply.accepted_proposal()), reply.accepted_value().c_str(),
      s.error_code(), reply.ret_val()));

    if (!s.ok()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }

    if (reply.ret_val() == S_NOT_LEADER) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      if (!is_leader(this_name)) {
        ++(*response_count);
        return std::make_unique<PvPair>(INVALID_PROPOSAL, "");
      }
      continue;
    }

    ++(*response_count);
    assert(reply.ret_val() == S_SUCCESS);

    uint64_t v = reply.accepted_proposal();
    const std::string &val = reply.accepted_value();
    return std::make_unique<PvPair>(v, val);
  }

  return std::make_unique<PvPair>(INVALID_PROPOSAL, "");
}

std::unique_ptr<uint64_t> PaxosService::send_accept_until(
           std::shared_ptr<MonitorInstance> instance, 
           const uint32_t log_index, 
           const uint32_t first_unchosen_index,
           const uint64_t proposal,
           const std::string &value, std::atomic<int> *response_count, 
           const int target_count) {
  monitor_rpc::AcceptRequest request;
  std::chrono::system_clock::time_point deadline = 
    std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
  request.set_log_index(log_index);
  request.set_proposal(proposal);
  request.set_value(value);
  request.set_proposer(this_name);
  request.set_first_unchosen_index(first_unchosen_index);

  while (response_count->load() < target_count) {
    grpc::ClientContext ctx;
    monitor_rpc::AcceptReply reply;

    ctx.set_deadline(deadline);

    logger->info(fmt::sprintf(
      "[ACCEPT] request to monitor[%s] index[%d] first_unchosen[%lu] proposal[%s] value[%s]",
      instance->info.name.c_str(), log_index, first_unchosen_index, uint64_two(proposal), value.c_str()
    ));

    auto s = instance->stub->accept(&ctx, request, &reply);

    if (!s.ok()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }

    logger->info(fmt::sprintf(
      "[ACCEPT] reply from monitor[%s] index[%d] first_unchosen[%lu] proposal[%s]. "
      " Result: rpc[%d], ret[%d], first_unchosen[%lu]",
      instance->info.name, log_index, first_unchosen_index, uint64_two(proposal), 
      s.error_code(), reply.ret_val(), reply.first_unchosen_index()
    ));

    if (reply.ret_val() == S_NOT_LEADER) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      if (!is_leader(this_name)) {
        ++(*response_count);
        return std::make_unique<uint64_t>(std::numeric_limits<uint64_t>::max());
      }
      continue;
    }

    set_instance_first_unchosen_index(instance->info.name, 
                                      reply.first_unchosen_index());

    ++(*response_count);
    assert(reply.ret_val() == S_SUCCESS);

    return std::make_unique<uint64_t>(reply.min_proposal());
  }

  return std::make_unique<uint64_t>(0);
}

void PaxosService::send_success(
           std::shared_ptr<MonitorInstance> instance, 
           const uint32_t log_index, 
           const std::string &value) {
  monitor_rpc::SuccessRequest request;
  request.set_log_index(log_index);
  request.set_value(value);

  while (running) {
    grpc::ClientContext ctx;
    monitor_rpc::SuccessReply reply;

    std::chrono::system_clock::time_point deadline = 
      std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
    ctx.set_deadline(deadline);

    logger->info(fmt::sprintf(
      "[SUCCESS] request to monitor[%s]: index[%d] value[%s]",
      instance->info.name.c_str(), log_index, value.c_str()
    ));

    auto status = instance->stub->success(&ctx, request, &reply);

    logger->info(fmt::sprintf(
      "[SUCCESS] reply from monitor[%s]. index[%d] "
      " Result: rpc[%d], first_unchosen[%lu]",
      instance->info.name, log_index,
      status.error_code(), reply.first_unchosen_index()
    ));

    if (!status.ok()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }

    set_instance_first_unchosen_index(instance->info.name, reply.first_unchosen_index());
    break;
  }
}

void PaxosService::sync_routine(PaxosService::SyncControlBlock *scb) {
  while (running) {
    while (running && scb->need_sync()) {
      uint32_t log_index = scb->get_first_unchosen_index();
      uint64_t log_proposal;
      std::string log_value;

      paxos->get_log(log_index, &log_proposal, &log_value);

      if (log_proposal != CHOSEN_PROPOSAL) {
        logger->info(fmt::sprintf("ERROR: log_proposal [%llu] != CHOSEN_PROPOSAL\n",
          log_proposal));
        assert(false);
      }

      send_success(scb->instance, log_index, log_value);
    }

    std::unique_lock<std::mutex> lock(scb->mutex);

    while (running) {
      scb->state_changed.wait_for(lock, std::chrono::milliseconds(200));
      if (paxos->get_first_unchosen_index() > scb->first_unchosen_index) {
        break;
      }
    };
  }
}

std::shared_ptr<MonitorInstance> PaxosService::get_leader() {
  std::vector<std::string> names;
  std::shared_ptr<MonitorInstance> leader;

  for (auto x = monitor_cluster->cluster_map.begin(); 
       x != monitor_cluster->cluster_map.end();
       ++x) {
    auto instance = x->second;

    if (!instance->is_alive(paxos::HEARTBEAT_TIMEOUT_INTERVAL)) {
      continue;
    }

    if (leader == nullptr ||
        x->second->info.name > leader->info.name) {
      leader = x->second;
    }
  }
  assert(leader != nullptr);
  return leader;
}

bool PaxosService::is_leader(const std::string &name) {
  return get_leader()->info.name == name;
}

void PaxosService::get_last_chosen_log(uint32_t *log_index, std::string *value) {
  paxos->get_last_chosen_log(log_index, value);
}

// TODO: this is dangerous.
//       how to prevent race condition?
//       one way is to lock each log, and serialize logs one by one
void PaxosService::get_serialized_logs(std::string *buf) const {
  paxos->get_logs(buf);
}

uint32_t PaxosService::get_first_unchosen_index() const {
  return paxos->get_first_unchosen_index();
}


} // namespace paxos
} // namespace morph