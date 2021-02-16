#ifndef MORPH_MDS_REQUEST_CACHE_H
#define MORPH_MDS_REQUEST_CACHE_H

#include <unordered_map>
#include <common/nocopy.h>
#include <common/types.h>
#include <common/utils.h>
#include <common/rpc_args.h>

namespace morph {

class RequestCache: NoCopy {
 public:
  template <typename T>
  int get_reply(cid_t cid, rid_t rid, T &reply) {
    auto x = cache.find(cid);
    if (x == cache.end()) {
      return -1;
    }
    auto y = x->second.find(rid);
    if (y == x->second.end()) {
      return -1;
    }

    deserialize(y->second, reply);
    return 0;
  }

  template <typename T>
  void set_reply(cid_t cid, rid_t rid, const T &reply) {
    cache[cid][rid] = serialize(reply);
  }

 private:
  std::unordered_map<cid_t, std::unordered_map<rid_t, std::string>> cache;
};

}

#endif