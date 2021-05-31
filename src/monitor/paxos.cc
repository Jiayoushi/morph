#include "paxos.h"

#include "common/logger.h"
#include "common/coding.h"

namespace morph {
namespace paxos {

Paxos::Paxos(const std::string &name):
  this_name(name),
  server_id(std::hash<std::string>()(name)) {

  logs.reserve(MAX_LOG_COUNT);

  for (uint32_t i = 0; i < MAX_LOG_COUNT; ++i) {
    logs.emplace_back(i);
  }
}

Log * Paxos::get_unchosen_log() {
  Log *log = nullptr;

  // Find first log entry that has is a clean slate
  for (uint32_t i = 0; i < logs.size(); ++i) {
    if (logs[i].state != VALUE_CHOSEN) {
      return &logs[i];
    }
  }

  assert(false);
}

void Paxos::prepare_handler(const uint32_t log_index,
                            const uint64_t proposal,
                            uint64_t *out_accepted_proposal, 
                            std::string *out_accepted_value) {
  assert(log_index <= MAX_LOG_COUNT);
  Log &log = logs[log_index];

  if (proposal > log.min_proposal) {
    log.min_proposal = proposal;
  }

  if (log.state == VALUE_CHOSEN) {
    *out_accepted_proposal = log.accepted_proposal;
    *out_accepted_value = log.accepted_value;
  } else {
    *out_accepted_proposal = INVALID_PROPOSAL;
    *out_accepted_value = "";
  }
}

void Paxos::accept_handler(const uint32_t log_index, const uint64_t proposal,
                           const std::string &value, uint64_t *min_proposal) {
  assert(log_index <= MAX_LOG_COUNT);
  Log &log = logs[log_index];

  if (proposal >= log.min_proposal) {
    log.min_proposal = proposal;
    log.set_accepted(proposal, value);
  }
  *min_proposal = log.min_proposal;
}

void Paxos::commit_handler(const uint32_t log_index,
                           const uint64_t proposal) {
  Log &log = logs[log_index];
  log.set_chosen();
}

uint64_t Paxos::choose_new_proposal_number(Log *log) {
  assert(log != nullptr);
  uint64_t proposal;

  do {
    ++log->max_round;
    proposal = to_proposal(log->max_round, server_id);
    assert(proposal != log->min_proposal);
  } while (proposal < log->min_proposal);

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
  for (uint32_t i = logs.size() - 1; i >= 0; --i) {
    if (logs[i].state == VALUE_CHOSEN) {
      *log_index = i;
      *value = logs[i].accepted_value;
      return;
    }
  }
  assert(false);
}

} // namespace paxos
} // namespace morph