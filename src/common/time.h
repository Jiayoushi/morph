#ifndef MORPH_COMMON_TIME_H
#define MORPH_COMMON_TIME_H

#include <chrono>

namespace morph {

using Timepoint = std::chrono::time_point<std::chrono::steady_clock>;

inline Timepoint now() {
  return std::chrono::steady_clock::now();
}

inline double elapsed_in_ms(Timepoint start) {
  Timepoint end = now();
  std::chrono::duration<double> diff = end - start;
  return diff.count() * 1000;
}



}


#endif