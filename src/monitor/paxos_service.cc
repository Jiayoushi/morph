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
  return paxos->accept_handler(log_index, proposal, value, min_proposal);
}

void PaxosService::prepare_handler(const uint32_t log_index, const uint64_t proposal, 
                                   uint64_t *out_accepted_proposal, 
                                   std::string *accepted_value) {
  return paxos->prepare_handler(log_index, proposal, out_accepted_proposal, 
                                accepted_value);
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
  if (pair->accepted_proposal != DEFAULT_PROPOSAL) {
    log->set_accepted(pair->accepted_proposal, pair->accepted_value);
    value_to_send = &pair->accepted_value;
  } else {
    value_to_send = &value;
  }

  bool chosen = broadcast_accept(log->get_log_index(), 
                                 proposal, *value_to_send);
  if (chosen) {
    log->set_chosen();
    *log_index = log->get_log_index();
  } else {
    log->reset();
  }
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

  std::vector<std::unique_ptr<PvPair>> pairs;
  for (auto &f: futures) {
    f.wait();
    assert(f.valid());
    pairs.push_back(std::move(f.get()));
  }

  sort(pairs.begin(), pairs.end(),
    [](const auto &p1, const auto &p2) {
      return p1->accepted_proposal > p2->accepted_proposal;
    });

  return std::move(pairs.back());
}

bool PaxosService::broadcast_accept(const uint32_t log_index,
                                    const uint64_t proposal, 
                                    const std::string &value) {
  std::atomic<int> response_count(0);
  auto &cluster_map = monitor_cluster->cluster_map;
  const int target_count = cluster_map.size() / 2;

  assert(cluster_map.size() % 2 != 0);

  // Right now the membership is not implemented.
  std::list<std::future<uint64_t>> futures;
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
    results.push_back(f.get());
  }

  for (uint64_t result: results) {
    if (result > proposal) {
      return false;
    }
  }

  return true;
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

    logger->info(fmt::sprintf("send_prepare to [%s]: log_index[%d] proposal[%lu] target_count[%d] \n",
      instance->info.name.c_str(), log_index, proposal, target_count));

    auto s = instance->stub->prepare(&ctx, request, &reply);
    if (!s.ok()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    uint64_t v = reply.accepted_proposal();
    const std::string &val = reply.accepted_value();
    return std::make_unique<PvPair>(v, val);
  }

  return nullptr;
}

uint64_t PaxosService::send_accept(
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

    auto s = instance->stub->accept(&ctx, request, &reply);
    if (!s.ok()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    return reply.min_proposal();
  }

  return DEFAULT_PROPOSAL;
}

bool PaxosService::is_leader(std::shared_ptr<std::vector<std::string>> names) {
  std::vector<std::string> copy = *names;
  sort(copy.begin(), copy.end());
  return copy.back() == this_name;
}


} // namespace paxos
} // namespace morph