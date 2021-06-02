#include "filename.h"

#include <iostream>
#include <cassert>

namespace morph {

static std::string make_file_name(const std::string &name, uint64_t number,
                                  const char* suffix) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/%06llu.%s",
                static_cast<unsigned long long>(number), suffix);
  return name + buf;
}

static std::string make_file_name(const std::string &name,
                                  const char* suffix) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/%s", suffix);
  return name + buf;
}

std::string log_file_name(const std::string &name, uint64_t number) {
  assert(number >= 0);
  return make_file_name(name, number, "log");
}

std::string paxos_file_name(const std::string &name) {
  return make_file_name(name, "paxos");
}

std::string logger_file_name(const std::string &name) {
  return make_file_name(name, "logger");
}

std::string block_store_file_name(const std::string &name) {
  return make_file_name(name, "block_store");
}

std::string kv_db_file_name(const std::string &name) {
  return make_file_name(name, "kv_db");
}

std::string kv_wal_file_name(const std::string &name) {
  return make_file_name(name, "kv_wal");
}

uint64_t get_filename_number(const std::string &filename) {
  size_t dot = filename.find('.');
  assert(dot != std::string::npos);
  return stoul(filename.substr(0, dot), 0, 10);
}

std::string get_filename_suffix(const std::string &filename) {
  size_t dot = filename.find_last_of('.');
  if (dot == std::string::npos) {
    return "";
  }
  return filename.substr(dot + 1, filename.size());
}

} // namespace morph