#ifndef MORPH_TEST_H
#define MORPH_TEST_H

#include <string>

namespace morph {

class Test {
 public:
  void test_rocksdb();
  void test_msgpack();

  void test_integration();
  void test_mkdir();
};

}

#endif