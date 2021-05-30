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
    *out_accepted_proposal = DEFAULT_PROPOSAL;
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

uint64_t Paxos::choose_new_proposal_number(Log *log) {
  assert(log != nullptr);
  char buf[8];
  char *buf_ptr = buf;

  ++log->max_round;
  encode_fixed_32(buf_ptr, server_id);
  encode_fixed_32(buf_ptr + 4, log->max_round);

  return decode_fixed_64(buf);
}

} // namespace paxos
} // namespace morph