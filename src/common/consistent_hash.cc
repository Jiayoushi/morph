#include "consistent_hash.h"

#include <vector>
#include <string>
#include <map>
#include <cassert>
#include <algorithm>

namespace morph {

static const int REPLICATION_FACTOR = 100;

std::vector<std::string> assign_group(const std::shared_ptr<std::vector<std::string>> &buckets,
                                     const std::string &item, const int size) {
  return assign_group(*buckets, item, size);
}

std::vector<std::string> assign_group(const std::vector<std::string> &buckets,
                                      const std::string &item, const int size) {
  std::vector<std::string> result;
  std::hash<std::string> hash_func;
  std::map<size_t, std::string> ring;

  assert(!buckets.empty());
  assert(size <= buckets.size());

  for (const std::string &bucket: buckets) {
    for (int i = 0; i < REPLICATION_FACTOR; ++i) {
      std::string id = bucket + std::to_string(i);
      size_t hash_val = hash_func(id);
      ring[hash_val] = bucket;
    }
  }

  for (int i = 0; i < size; ++i) {
    std::string id = item + std::to_string(i);
    size_t hash_val = hash_func(item);
    auto iter = ring.lower_bound(hash_val);

    if (iter == ring.end()) {
      iter = ring.begin();
    }

    while (std::find(std::begin(result), std::end(result), iter->second) != std::end(result)) {
      ++iter;
      if (iter == ring.end()) {
        iter = ring.begin();
      }
    }

    result.push_back(iter->second);
  }

  assert(result.size() == size);
  return result;
}

} // namespace morph