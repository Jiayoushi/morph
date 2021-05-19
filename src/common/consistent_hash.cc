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
  std::map<size_t, std::string> sorted_hash_vals;
  std::vector<std::string> result;

  assert(!buckets.empty());
  assert(size <= buckets.size());

  for (int i = 0; i < buckets.size(); ++i) {
    long hash_val = hash_string(buckets[i].c_str());
    sorted_hash_vals.insert({hash_val, buckets[i]});
  }

  long hash_val = hash_string(item.c_str());
  auto p = sorted_hash_vals.lower_bound(hash_val);

  for (int i = 0; i < size; ++i) {
    while (true) {
       if (p == sorted_hash_vals.end()) {
        p = sorted_hash_vals.begin();
      }
      if (std::find(result.begin(), result.end(), p->second) == end(result)) {
        break;
      }
      ++p;
    }
    result.push_back(p->second);
  }

  assert(result.size() == size);
  return result;
}

} // namespace morph