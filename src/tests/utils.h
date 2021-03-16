#ifndef MORPH_TEST_UTILS_H
#define MORPH_TEST_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <algorithm>


namespace morph {

char *get_garbage(char *buf, size_t size) {
  char c;
  for (int i = 0; i < size; ++i) {
    c = 97 + (rand() % 26);
    *buf++ = c;
  }
  return buf;
}

}

#endif