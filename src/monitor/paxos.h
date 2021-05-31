#ifndef MORPH_PAXOS_PAXOS_H
#define MORPH_PAXOS_PAXOS_H

#include <rpc/msgpack.hpp>

#include "common/logger.h"

namespace morph {
namespace paxos {

// This is a proposal number that is invalid.
// Valid numbers > this number.
const uint64_t INVALID_PROPOSAL = 0;

class Paxos;

enum LogState {
  NO_VALUE = 0,   // No value assigned

  VALUE_ACCEPTED = 1,

  VALUE_CHOSEN = 2,
};

struct Log {
  LogState state;

  Log():
    log_index(0), state(NO_VALUE), max_round(0), 
    min_proposal(INVALID_PROPOSAL), accepted_proposal(INVALID_PROPOSAL), 
    accepted_value() {}

  Log(const uint32_t log_index):
      Log() {
    this->log_index = log_index;
  }

  void set_accepted() {
    state = VALUE_ACCEPTED;
  }

  void set_accepted(const uint64_t proposal, const std::string &value) {
    state = VALUE_ACCEPTED;
    accepted_proposal = proposal;
    accepted_value = value;
  }

  void set_chosen() {
    state = VALUE_CHOSEN;
  }

  void reset() {
    log_index = 0;
    state = NO_VALUE;
    max_round = 0;
    min_proposal = INVALID_PROPOSAL;
    accepted_proposal = INVALID_PROPOSAL;
    accepted_value.clear();
  }

  uint32_t get_log_index() const {
    return log_index;
  }

 
  // Use this function to persist the states. Do not directly using msgpack
  // since it only include some parts of the state.
  void persist() {
    assert(false && "NOT YET IMPLEMENTED");
  }

  MSGPACK_DEFINE_ARRAY(accepted_proposal, accepted_value);

 private:

  friend class Paxos;
  
  uint32_t log_index;

  uint32_t max_round;

  // Highest proposal we have seen so far.
  // We will not accept any proposal that is <= than this value.
  uint64_t min_proposal;

  uint64_t accepted_proposal;

  std::string accepted_value;

};

class Paxos {
 public:
  Paxos(const std::string &name);

  Log * get_unchosen_log();

  uint64_t choose_new_proposal_number(Log *log);

  void prepare_handler(const uint32_t log_index, const uint64_t proposal, 
                       uint64_t *out_accepted_proposal, 
                       std::string *accepted_value);

  void accept_handler(const uint32_t log_index,
                      const uint64_t proposal, const std::string &value,
                      uint64_t *min_proposal);

  void commit_handler(const uint32_t log_index,
                       const uint64_t proposal);

  void get_last_chosen_log(uint32_t *log_index, std::string *value);

  MSGPACK_DEFINE_ARRAY(logs);

 private:
  void run(Log *log, const std::string &value);

  uint64_t to_proposal(const uint32_t max_round, const uint32_t server_id);


  std::shared_ptr<spdlog::logger> logger;

  const uint32_t server_id;

  const std::string this_name;

  // TODO(REQUIRED): it's capped.
  const static int MAX_LOG_COUNT = 1000;

  std::vector<Log> logs;
};

} // namespace paxos
} // namespace morph


#endif

