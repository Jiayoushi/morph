#include "block_allocator.h"

namespace morph {

namespace os {

lbn_t BlockAllocator::allocate_blocks(const uint32_t count, 
                                      LogHandle *log_handle) {
  std::string key;
  std::string value;
  lbn_t start_lbn;
  
  start_lbn = allocate(count);

  if (log_handle) {
    log(start_lbn, count, log_handle);
  }

  return start_lbn;
}

lbn_t BlockAllocator::allocate(const uint32_t count) {
  lbn_t lbn;
  uint8_t retry;

  for (retry = 0; retry < MAX_RETRY; ++retry) {
    {
      std::unique_lock<std::mutex> lock(bitmap_mutex);
  
      if (free_blocks_cnt >= count) {
        if (find_free(count, &lbn)) {
          free_blocks_cnt -= count;
          set_values(lbn, count, USED);
          break;
        }
      }
    }

    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  // TODO: external fragmentation or simply out of memory
  //       the latter probably does not need to be addressed, but the first one.
  assert(retry != MAX_RETRY);

  return lbn;
}

void BlockAllocator::deallocate_blocks(const lbn_t start_block, 
                                       const uint32_t count, 
                                       LogHandle *log_handle) {
  std::unique_lock<std::mutex> lock(bitmap_mutex);
  free_blocks_cnt += count;
  set_values(start_block, count, FREE);

  if (log_handle) {
    log(start_block, count, log_handle);
  }
}

void BlockAllocator::log(const lbn_t start_block, const uint32_t count, 
                         LogHandle *log_handle) const {
  assert(log_handle != nullptr);

  uint64_t start_key = start_block / BLOCKS_PER_KEY;
  uint64_t end_key = (start_block + count) / BLOCKS_PER_KEY;

  for (uint64_t key = start_key; key <= end_key; ++key) {
    // TODO(URGENT): it implies the maximum key is 64 bits. 
    //               It's kinda dangerous, use char buf[...] later.
    std::string key_str = std::to_string(key);
    std::string value = bits[key].to_string();

    log_handle->put(CF_SYS_BITMAP, key_str, value);
  }
}

bool BlockAllocator::find_free(uint32_t count, lbn_t *allocated_start) {
  uint32_t cnt = 0;

  for (lbn_t lbn = 0; lbn < bits.size() * 8; ++lbn) {
    if (get_value(lbn) == USED) {
      cnt = 0;
      continue;
    }

    if (++cnt == count) {
      *allocated_start = lbn - count + 1;
      return true;
    }
  }

  return false;
}

void BlockAllocator::restore_metadata(const std::string &key, 
                                      const std::string &value) {
  assert(!key.empty());
  assert(value.size() == BLOCKS_PER_KEY);

  uint64_t key_int;

  {
    std::istringstream iss(key);
    iss >> key_int;
  }

  const char *c = value.c_str();
  for (int i = 0; i < BLOCKS_PER_KEY; ++i) {
    bits[key_int].set(i, *c == 1);
    ++c;
  }
  assert(*c == '\0');
}

std::string BlockAllocator::serialize() {
  std::stringstream ss;
  std::vector<unsigned long> vs;

  for (const std::bitset<8> &set: bits) {
    vs.push_back(set.to_ulong());
  }

  clmdep_msgpack::pack(ss, vs);
  return ss.str();
}

void BlockAllocator::deserialize(const std::string &v) {
  std::vector<unsigned long> out;

  clmdep_msgpack::object_handle handle = clmdep_msgpack::unpack(v.data(), v.size());
  clmdep_msgpack::object deserialized = handle.get();
  deserialized.convert(out);

  // You must set the total blocks to the previous value
  assert(out.size() == bits.size());

  bits.clear();
  for (uint32_t i = 0; i < out.size(); ++i) {
    bits.emplace_back(out[i]);
  }
}

} // namespace os

} // namespace morph