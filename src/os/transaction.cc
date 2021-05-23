#include "transaction.h"

#include "kv_store.h"

namespace morph {

namespace os {

void LogHandle::put(CF_INDEX index, const std::string &key, 
                    const std::string &value) {
  auto s = write_batch.Put(kv_store->get_cf_handle(index), key, value);

  if (!s.ok()) {
    std::cerr << "Failed to put: " << s.ToString() << std::endl;
    exit(EXIT_FAILURE);
  }
}

void LogHandle::merge(CF_INDEX index, const std::string &key, 
                      const std::string &value) {
  auto s = write_batch.Merge(kv_store->get_cf_handle(index), key, value);
  if (!s.ok()) {
    std::cerr << "Failed to merge: " << s.ToString() << std::endl;
    exit(EXIT_FAILURE);
  }
}


} // namespace os

} // namespace moprh