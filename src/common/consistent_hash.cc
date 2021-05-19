#include "consistent_hash.h"

#include <vector>
#include <string>
#include <map>
#include <cassert>
#include <algorithm>

namespace morph {

// Horner’s rule
long hash_string(const char *key) {
  long hash_val = 0;
	while (*key != '\0') {
		hash_val = (hash_val << 4) + *(key++);
		long g = hash_val & 0xF0000000L;
		if (g != 0) 
      hash_val ^= (g >> 24);
		hash_val &= ~g;
	}
	return hash_val;
}

std::vector<std::string> assign_group(const std::vector<std::string> &buckets,
                                      const std::string &item, const int size) {
  std::vector<std::string> sorted_buckets;
  std::vector<std::string> result;

  assert(!buckets.empty());
  assert(size <= buckets.size());

  sorted_buckets = buckets;
  std::sort(sorted_buckets.begin(), sorted_buckets.end());

  long hash_val = hash_string(item.c_str());
  int index = hash_val % sorted_buckets.size();
  for (int i = 0; i < size; ++i) {
    result.push_back(sorted_buckets[index]);
    index = (index + 1) % sorted_buckets.size();
  }

  assert(result.size() == size);
  return result;
}

} // namespace morph