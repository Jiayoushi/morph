#include <gtest/gtest.h>

#include <unordered_set>
#include <unordered_map>

#include "common/consistent_hash.h"
#include "tests/utils.h"

namespace morph {
namespace test {

TEST(ConsistentHash, NoDuplicates) {
  std::vector<std::string> names = {"oss0", "oss1", "oss2", "oss3", 
                                    "oss4", "oss5"};
  auto no_dup = [](const std::vector<std::string> &x) {
    std::unordered_set<std::string> set(x.begin(), x.end());
    return set.size() == x.size();
  };

  for (int i = 0; i < 10000; ++i) {
    std::string obj_name = "obj" + std::to_string(i);
    ASSERT_TRUE(no_dup(assign_group(names, obj_name, 3)));
  }
}

TEST(ConsistentHash, Uniform) {
  std::vector<std::string> names;
  std::unordered_map<std::string, int> load;
  const int TOTAL_OBJECTS = 10000;
  const int INITIAL_SERVERS = 10;
  const double DEVIATION_FACTOR = 0.15;

  for (int i = 1; i <= INITIAL_SERVERS; ++i) {
    names.push_back("oss" + std::to_string(i));
  }

  for (int x = 0; x < 5; ++x) {
    const int EXPECTED_LOAD = 3 * (TOTAL_OBJECTS / names.size());
    const int MAX_ALLOWED_DIFF = EXPECTED_LOAD * DEVIATION_FACTOR;

    for (int i = 0; i < TOTAL_OBJECTS; ++i) {
      std::string obj_name = get_garbage(10);
      for (const auto &x: assign_group(names, obj_name, 3)) {
        load[x] += 1;
      }
    }

    for (auto p = load.cbegin(); p != load.cend(); ++p) {
      EXPECT_TRUE(abs(p->second -  EXPECTED_LOAD) <= MAX_ALLOWED_DIFF);
    }

    names.erase(names.begin() + (x % names.size()));
    load.clear();
  }
}

}
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}