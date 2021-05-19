#ifndef MORPH_DISTRIBUTION_CONSISTENT_HASH_H
#define MORPH_DISTRIBUTION_CONSISTENT_HASH_H

#include <vector>
#include <string>
#include <map>
#include <cassert>

namespace morph {

int assign(const std::vector<std::string> &buckets,
           const std::string &item);

// Call this to assign the primary-backup oss, simply adding suffix
// to name will lead to collision
std::vector<std::string> assign_group(const std::vector<std::string> &buckets,
                                     const std::string &item, const int size=3);

} // namespace morph

#endif