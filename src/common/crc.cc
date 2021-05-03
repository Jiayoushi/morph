// Author: https://lxp32.github.io/docs/a-simple-example-crc32-calculation/

#include "crc.h"

#include "coding.h"

namespace morph {

namespace crc32c {

static bool table_created = false;
static uint32_t crc32_table[256];

void build_crc32_table() {
  for (uint32_t i = 0; i < 256; i++) {
	uint32_t ch=i;
	uint32_t crc=0;
	for (size_t j = 0; j < 8; j++) {
	  uint32_t b = (ch^crc) & 1;
	  crc >>= 1;
	  if (b) 
        crc = crc^0xEDB88320;
	  ch >>= 1;
	}
	crc32_table[i] = crc;
  }
}

static void compute(uint32_t *crc, const char *s, size_t n) {
  for (size_t i = 0; i < n; i++) {
    char ch = s[i];
    uint32_t t = (ch^(*crc)) & 0xFF;
    *crc = ((*crc) >> 8)^crc32_table[t];
  }
}

uint32_t crc32_fast(const char *s, size_t n) {
  uint32_t crc = 0xFFFFFFFF;

  if (!table_created) {
    build_crc32_table();
    table_created = true;
  }
  compute(&crc, s, n);
  return ~crc;
}

uint32_t crc32_fast(uint32_t crc, const char *s, size_t n) {
  if (!table_created) {
    build_crc32_table();
    table_created = true;
  }
  compute(&crc, s, n);
  return ~crc;
}

} // namespace crc32c

} // namespace morph