#ifndef MORPH_PAXOS_PAXOS_H
#define MORPH_PAXOS_PAXOS_H

#include <rpc/msgpack.hpp>

#include "common/logger.h"
#include "persister.h"

namespace morph {
namespace paxos {

// This is a proposal number that is invalid.
// Valid numbers > this number.
const uint64_t INVALID_PROPOSAL = 0;
const uint64_t CHOSEN_PROPOSAL = std::numeric_limits<uint64_t>::max();

class Paxos;

struct Log {
  Log():
    accepted_proposal(INVALID_PROPOSAL), 
    accepted_value() {}

  MSGPACK_DEFINE_ARRAY(accepted_proposal, accepted_value);

 private:
  friend class Paxos;

  uint64_t accepted_proposal;

  std::string accepted_value;
};


class Paxos {
 public:
  Paxos(const std::string &name);

  ~Paxos();

  Log * get_unchosen_log();

  uint64_t choose_new_proposal_number();

  void prepare_handler(const uint32_t log_index, const uint64_t proposal, 
                       uint64_t *out_accepted_proposal, 
                       std::string *accepted_value);

  void accept_handler(const uint32_t log_index, const uint32_t first_unchosen,
                      const uint64_t proposal, const std::string &value,
                      uint64_t *min_proposal);

  void success_handler(const uint32_t log_index,
                       const std::string &value);

  void get_last_chosen_log(uint32_t *log_index, std::string *value);

  uint32_t get_first_unchosen_index() const {
    return first_unchosen_index;
  }
  
  void set_accepted(Log *log, const uint64_t proposal, const std::string &value) {
    std::lock_guard<std::recursive_mutex> lock(mutex);

    log->accepted_proposal = proposal;
    log->accepted_value = value;
    persist();
  }

  void set_chosen(Log *log, const std::string *value) {
    std::lock_guard<std::recursive_mutex> lock(mutex);

    log->accepted_proposal = CHOSEN_PROPOSAL;
    if (value != nullptr) {
      log->accepted_value = *value;
    }

    for (int i = 0; i < logs.size(); ++i) {
      if (!chosen(&logs[i])) {
        first_unchosen_index.store(i);
        persist();
        break;
      }
    }
  }

  bool chosen(const Log *log) const {
    return log->accepted_proposal == CHOSEN_PROPOSAL;
  }

  bool has_accepted_proposal(const Log *log) const {
    return log->accepted_proposal != INVALID_PROPOSAL;
  }

  void reset(Log *log) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    log->accepted_proposal = INVALID_PROPOSAL;
    log->accepted_value.clear();
    persist();
  }

  uint32_t get_log_index(const Log *log) const {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    return log - &logs[0];
  }

  Log * get_log(const uint32_t log_index) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    assert(log_index < logs.size());
    return &logs[log_index];
  }

  void get_log(const uint32_t log_index, uint64_t *log_proposal, 
               std::string *value) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    assert(log_index < logs.size());
    *log_proposal = logs[log_index].accepted_proposal;
    *value = logs[log_index].accepted_value;
  }
 
  void get_logs(std::string *buf) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    std::stringstream ss;

    for (int i = 0; i < MAX_LOG_COUNT; ++i) {
      clmdep_msgpack::pack(ss, logs[i]);
    }
    *buf = std::move(ss.str());
  }

  void get_min_pro_and_first_unchosen(uint64_t *min_prop, 
                                      uint32_t *first_unchosen) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    *min_prop = min_proposal;
    *first_unchosen = first_unchosen_index;
  }


  MSGPACK_DEFINE_ARRAY(min_proposal, logs);

 private:
  void run(Log *log, const std::string &value);

  uint64_t to_proposal(const uint32_t max_round, const uint32_t server_id);

  void serialize(std::string *buf) {
    std::stringstream ss;
    clmdep_msgpack::pack(ss, *this);
    *buf = std::move(ss.str());
  }

  void deserialize(const std::string &s) {
    using namespace clmdep_msgpack;
    object_handle oh = unpack(s.data(), s.size());
    object obj = oh.get();
    obj.convert(*this);
  }

  // Use this function to persist the states.
  void persist() {
    std::string buf;
    serialize(&buf);
    persister.persist(std::move(buf));
  }

  mutable std::recursive_mutex mutex;

  std::shared_ptr<spdlog::logger> logger;

  const uint32_t server_id;

  const std::string this_name;

  // TODO(REQUIRED): it's capped.
  const static int MAX_LOG_COUNT = 1000;

  std::vector<Log> logs;

  // This server will generate proposals that are bigger than this
  // Monotonically increased
  uint64_t min_proposal;

  std::atomic<uint32_t> first_unchosen_index;

  Persister persister;
};

} // namespace paxos
} // namespace morph


#endif

