#ifndef MORPH_TYPES_H
#define MORPH_TYPES_H

#include <sys/types.h>
#include <rpc/server.h>

#include "common/options.h"
#include "slice.h"

namespace morph {
namespace mds {

// TODO: use capitalized letters...
using op_t   = uint32_t;
using hid_t  = uint64_t;        // Handle id
using rid_t  = uint64_t;        // Request id
using type_t = uint8_t;         // Type of an inode

using LogBuffers = std::string;

using SequenceNumber = uint64_t;

using Operation = std::tuple<int, Slice, Slice>;

using OpVector = std::vector<Operation>;

} // namespace mds

namespace os {

using lbn_t  = uint32_t;        // Logical Block Number

} // namespace os

} // namespace morph

#endif
