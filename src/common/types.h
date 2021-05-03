#ifndef MORPH_TYPES_H
#define MORPH_TYPES_H

#include <sys/types.h>
#include <rpc/server.h>
#include <common/options.h>

namespace morph {

// TODO: use capitalized letters...
using op_t   = uint32_t;
using hid_t  = uint64_t;        // Handle id
using rid_t  = uint64_t;        // Request id
using lbn_t  = uint32_t;        // Logical Block Number
using type_t = uint8_t;         // Type of an inode

using LogBuffers = std::string;

}

#endif
