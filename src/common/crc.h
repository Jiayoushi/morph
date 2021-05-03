// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors
//
// Code modified.

#ifndef MORPH_CRC_H
#define MORPH_CRC_H

#include <stddef.h>
#include <stdint.h>

namespace morph {

namespace crc32c {

static const uint32_t MASK_DELTA = 0xa282ead8ul;

uint32_t crc32_fast(const char *s, size_t n);

uint32_t crc32_fast(uint32_t crc, const char *s, size_t n);

// Return a masked representation of crc.
//
// Motivation: it is problematic to compute the CRC of a string that
// contains embedded CRCs.  Therefore we recommend that CRCs stored
// somewhere (e.g., in files) should be masked before being stored.
inline uint32_t mask(uint32_t crc) {
  // Rotate right by 15 bits and add a constant.
  return ((crc >> 15) | (crc << 17)) + MASK_DELTA;
}

// Return the crc whose masked representation is masked_crc.
inline uint32_t unmask(uint32_t masked_crc) {
  uint32_t rot = masked_crc - MASK_DELTA;
  return ((rot >> 17) | (rot << 15));
}

} // namespace crc32c

} // namespace morph

#endif