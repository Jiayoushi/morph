#ifndef MORPH_TEST_UTILS_H
#define MORPH_TEST_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <algorithm>
#include <string>

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

}

#endif