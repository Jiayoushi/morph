#include "paxos.h"

#include "common/logger.h"
#include "common/coding.h"


namespace morph {
namespace paxos {

Paxos::Paxos(const std::string &name):
    this_name(name),
    server_id(std::hash<std::string>()(name)),
    max_round(0), min_proposal(INVALID_PROPOSAL),
    first_unchosen_index(0),
    persister(paxos_file_name(name)) {

  logger = init_logger(this_name);
  assert(logger != nullptr);


  if (!persister.empty()) {
    std::string value;
    persister.get_persisted(&value);
    deserialize(value);

    assert(logs.size() == MAX_LOG_COUNT);
    for (int i = 0; i < MAX_LOG_COUNT; ++i) {
      if (!chosen(&logs[i])) {
        first_unchosen_index = i;
        break;
      }
    }
  } else {
    logs.reserve(MAX_LOG_COUNT);
    for (uint32_t i = 0; i < MAX_LOG_COUNT; ++i) {
      logs.emplace_back(i);
    }
  }

  std::string v;
  persister.get_persisted(&v);

  logger->info(fmt::sprintf(
    "Paxos initialization completed: max_round[%lu] min_proposal[%zu] "
    "first_unchosen_index[%zu] persister_size[%zu] hash[%zu]",
    max_round, min_proposal, first_unchosen_index, persister.size(),
    std::hash<std::string>()(v)));
}

Paxos::~Paxos() {
  std::string v;
  persister.get_persisted(&v);

  logger->info(fmt::sprintf(
    "Exit... max_round[%lu] min_proposal[%zu] "
    "first_unchosen_index[%zu] persister_size[%zu] hash[%zu]\n\n\n\n",
    max_round, min_proposal, first_unchosen_index, persister.size(),
    std::hash<std::string>()(v)));
}


Log * Paxos::get_unchosen_log() {
  std::lock_guard<std::recursive_mutex> lock(mutex);

  // Find first log entry that has is a clean slate
  for (Log &log: logs) {
    if (!chosen(&log)) {
      return &log;
    }
  }

  assert(false);
}

void Paxos::prepare_handler(const uint32_t log_index,
                            const uint64_t proposal,
                            uint64_t *out_accepted_proposal, 
                            std::string *out_accepted_value) {
  std::lock_guard<std::recursive_mutex> lock(mutex);

  assert(log_index <= MAX_LOG_COUNT);
  Log &log = logs[log_index];

  if (proposal > min_proposal) {
    min_proposal = proposal;
  }

  if (chosen(&log)) {
    *out_accepted_proposal = log.accepted_proposal;
    *out_accepted_value = log.accepted_value;
  } else {
    *out_accepted_proposal = INVALID_PROPOSAL;
    *out_accepted_value = "";
  }
}

void Paxos::accept_handler(const uint32_t log_index, 
                           const uint32_t first_unchosen,
                           const uint64_t proposal,
                           const std::string &value, 
                           uint64_t *min_pro_out) {
  std::lock_guard<std::recursive_mutex> lock(mutex);

  assert(log_index <= MAX_LOG_COUNT);
  Log &log = logs[log_index];

  if (proposal > min_proposal) {
    min_proposal = proposal;
    set_accepted(&log, proposal, value);
  }
  *min_pro_out = min_proposal;

  // Process first_unchosen
  for (int i = 0; i < first_unchosen; ++i) {
    if (!chosen(&logs[i]) && logs[i].accepted_proposal == proposal) {
      set_chosen(&logs[i], nullptr);
    }
  }
}

void Paxos::success_handler(const uint32_t log_index,
                            const std::string &value) {
  set_chosen(&logs[log_index], &value);
}

uint64_t Paxos::choose_new_proposal_number() {
  std::lock_guard<std::recursive_mutex> lock(mutex);

  assert(log != nullptr);
  uint64_t proposal;

  do {
    ++max_round;
    proposal = to_proposal(max_round, server_id);
    assert(proposal != min_proposal);
  } while (proposal < min_proposal);

  persist();
  return proposal;
}

uint64_t Paxos::to_proposal(const uint32_t max_round, const uint32_t server_id) {
  uint64_t a = max_round;
  a <<= 32;
  a += std::numeric_limits<uint32_t>::max();

  uint64_t b = std::numeric_limits<uint32_t>::max();
  b <<= 32;
  b += server_id;

  return a & b;
}

void Paxos::get_last_chosen_log(uint32_t *log_index, std::string *value) {
  std::lock_guard<std::recursive_mutex> lock(mutex);

  for (uint32_t i = logs.size() - 1; i >= 0; --i) {
    if (chosen(&logs[i])) {
      *log_index = i;
      if (value != nullptr) {
        *value = logs[i].accepted_value;
      }
      return;
    }
  }
  assert(false);
}

} // namespace paxos
} // namespace morph