// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format

#ifndef MORPH_CODING_H
#define MORPH_CODING_H

#include <cstdint>

#include "slice.h"

namespace morph {

inline void encode_fixed_64(char* dst, uint64_t value) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

  // Recent clang and gcc optimize this to a single mov / str instruction.
  buffer[0] = static_cast<uint8_t>(value);
  buffer[1] = static_cast<uint8_t>(value >> 8);
  buffer[2] = static_cast<uint8_t>(value >> 16);
  buffer[3] = static_cast<uint8_t>(value >> 24);
  buffer[4] = static_cast<uint8_t>(value >> 32);
  buffer[5] = static_cast<uint8_t>(value >> 40);
  buffer[6] = static_cast<uint8_t>(value >> 48);
  buffer[7] = static_cast<uint8_t>(value >> 56);
}

void encode_fixed_32(char* dst, uint32_t value);

char * encode_varint32(char *dst, uint32_t v);

const char * get_varint32ptr_fallback(const char *p, const char *limit, 
  uint32_t *value);

const char * get_varint32ptr(const char *p, const char *limit, uint32_t *value);

bool get_varint32(Slice *input, uint32_t *value);

void put_varint32(std::string *dst, uint32_t v);

uint32_t decode_fixed_32(const char *ptr);


} // namespace morph

#endif