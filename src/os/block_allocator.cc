#include <os/block_allocator.h>

namespace morph {

pbn_t Bitmap::allocate_blocks(uint32_t count) {
  pbn_t pbn;
  int res;
  uint8_t retry;

  for (retry = 0; retry < MAX_RETRY; ++retry) {
    {
      std::unique_lock<std::mutex> lock(bitmap_mutex);
  
      if (free_blocks_cnt >= count) {
        res = find_free(count, pbn);
        if (res == 0) {
          free_blocks_cnt -= count;
          set_values(pbn, count, USED);

          break;
        }
      }
    }

    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }

  // TODO: external fragmentation or simply out of memory
  //       the latter probably does not need to be addressed, but the first one.
  assert(retry != MAX_RETRY);

  return pbn;
}

int Bitmap::find_free(uint32_t count, pbn_t &allocated_start) {
  uint32_t cnt = 0;

  for (pbn_t pbn = 0; pbn < bits.size() * 8; ++pbn) {
    if (get_value(pbn) == USED) {
      cnt = 0;
      continue;
    }

    if (++cnt == count) {
      allocated_start = pbn - count + 1;
      return 0;
    }
  }

  return -1;
}

std::string Bitmap::serialize() {
  std::stringstream ss;
  std::vector<unsigned long> vs;

  for (const std::bitset<8> &set: bits) {
    vs.push_back(set.to_ulong());
  }

  clmdep_msgpack::pack(ss, vs);
  return ss.str();
}

void Bitmap::deserialize(const std::string &v) {
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


}