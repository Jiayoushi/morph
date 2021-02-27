#ifndef MORPH_COMMON_CRC_H
#define MORPH_COMMON_CRC_H

#include <stddef.h>
#include <stdint.h>

namespace morph {

void build_crc32_table();
uint32_t crc32_fast(const char *s, size_t n);


}

#endif