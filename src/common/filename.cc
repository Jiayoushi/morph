#include "filename.h"

#include <cassert>

namespace morph {

static std::string make_file_name(const std::string &name, uint64_t number,
    const char* suffix) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/%06llu.%s",
                static_cast<unsigned long long>(number), suffix);
  return name + buf;
}

std::string log_file_name(const std::string &name, uint64_t number) {
  assert(number >= 0);
  return make_file_name(name, number, "log");
}

uint64_t get_number(const std::string &filename) {
  size_t dot = filename.find('.');
  assert(dot != std::string::npos);
  return stoul(filename.substr(0, dot), 0, 10);
}

} // namespace morph