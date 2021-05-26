#ifndef MORPH_TEST_UTILS_H
#define MORPH_TEST_UTILS_H

#include <memory>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <string>
#include <dirent.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace morph {

char *get_garbage(char *buf, size_t size) {
  char c;
  for (int i = 0; i < size; ++i) {
    c = 97 + (rand() % 26);
    *buf++ = c;
  }
  return buf;
}

std::string get_garbage(size_t size) {
  char buf[size];
  char c;

  for (int i = 0; i < size; ++i) {
    c = 97 + (rand() % 26);
    buf[i] = c;
  }
  
  return std::string(buf, size);
}

void get_garbage(std::string &s) {
   for (int i = 0; i < s.size(); ++i) {
    s[i] = 97 + (rand() % 26);
  }
}

void delete_directory(const std::string &pathname) {
  ::DIR *dir;
  struct ::dirent *entry;
  std::string entry_full_pathname;
   
  dir = opendir(pathname.c_str());
  if (dir == nullptr) {
    perror(pathname.c_str());
    exit(EXIT_FAILURE);
  } 
  
  while (true) {
    entry = readdir(dir);
    if (entry == nullptr) {
      break;
    }

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }

    entry_full_pathname = pathname + "/" + entry->d_name;

    if (entry->d_type == DT_DIR) {
      delete_directory(entry_full_pathname.c_str());
    } else {

      if (::unlink(entry_full_pathname.c_str()) < 0) {
        perror(entry_full_pathname.c_str());
        exit(EXIT_FAILURE);
      }
    }
  }

  if (rmdir(pathname.c_str()) < 0) {
    perror("rmdir failed");
    exit(EXIT_FAILURE);
  }

  closedir(dir);
}

std::shared_ptr<spdlog::logger> create_test_logger(const std::string &test_name) {
 std::shared_ptr<spdlog::logger> logger; 

  try {
    std::string filepath = "test_" + test_name;
    logger = spdlog::basic_logger_mt(test_name, filepath, true);
    logger->set_level(spdlog::level::level_enum::debug);
    logger->flush_on(spdlog::level::level_enum::debug);
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "Failed to create test logger: "<< ex.what() << std::endl;
    exit(EXIT_FAILURE);
  }

  return logger;
}

}

#endif
