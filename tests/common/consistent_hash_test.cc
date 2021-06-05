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
    auto group =assign_group(names, obj_name, 3); 
    ASSERT_TRUE(no_dup(group));
    for (const auto &x: group) {
      ASSERT_TRUE(std::find(names.begin(), names.end(), x) != names.end());
    }
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

TEST(ConsistentHash, RelocationCost) {
  std::vector<std::string> names;
  std::unordered_map<int, std::vector<std::string>> assigned;
  const int TOTAL_OBJECTS = 10000;
  const int INITIAL_SERVERS = 10;
  std::vector<std::string> obj_names;

  for (int i = 1; i <= INITIAL_SERVERS; ++i) {
    names.push_back("oss" + std::to_string(i));
  }
  obj_names.reserve(TOTAL_OBJECTS);
  for (int i = 0; i < TOTAL_OBJECTS; ++i) {
    obj_names.push_back(get_garbage(10));
  }

  for (int x = 0; x < 5; ++x) {
    std::unordered_map<int, std::vector<std::string>> new_assigned;

    new_assigned.reserve(TOTAL_OBJECTS);
    for (int i = 0; i < TOTAL_OBJECTS; ++i) {
      new_assigned[i] = assign_group(names, obj_names[i], 3);
    }

    if (x != 0) {
      const int EXPECTED_RELOCATION_OBJECTS = 3 * (TOTAL_OBJECTS / names.size());
      const int DEVIATION_FACTOR = 0.15;
      int diff = 0;

      for (int i = 0; i < TOTAL_OBJECTS; ++i) {
        const auto &old_group = assigned[i];
        const auto &new_group = new_assigned[i];

        for (const auto &x: new_group) {
          if (std::find(old_group.begin(), old_group.end(), x) == old_group.end()) {
            ++diff;
          }
        }
      }

      EXPECT_TRUE(diff <= EXPECTED_RELOCATION_OBJECTS * 1.15);
    }

    assigned = std::move(new_assigned);
    names.erase(names.begin() + (x % names.size()));
  }
}

}
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}