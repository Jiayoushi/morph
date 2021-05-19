#ifndef MORPH_MONITOR_CONFIG_H
#define MORPH_MONITOR_CONFIG_H

#include <vector>

#include "common/cluster.h"

namespace morph {
namespace monitor {


// This is the initial monitor setup passed to create a mds or oss
// It must include all the monitors that may be a leader.
struct Config {
  std::vector<Info> infos;
};

} // monitor
} // morph

#endif