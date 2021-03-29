#ifndef MORPH_COMMON_CRC_H
#define MORPH_COMMON_CRC_H

#include <stddef.h>
#include <stdint.h>

namespace morph {

uint32_t crc32_fast(const char *s, size_t n);


}

#endif