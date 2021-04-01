#ifndef MORPH_TYPES_H
#define MORPH_TYPES_H

#include <sys/types.h>
#include <rpc/server.h>
#include <common/options.h>

namespace morph {

using cid_t  = uint64_t;        // Client's id
using type_t = uint8_t;
using op_t   = uint32_t;
using hid_t  = uint64_t;        // Handle id
using rid_t  = uint64_t;        // Request id
using lbn_t  = uint32_t;        // Logical Block Number
using off_t  = uint32_t;

using LogBuffers = std::string;

// TODO: gotta move this somewhere else
struct stat {
  ino_t  st_ino;
  mode_t st_mode;
  uid_t  st_uid;
  MSGPACK_DEFINE_ARRAY(st_ino, st_mode, st_uid);
};

struct DIR {
  char pathname[PATHNAME_LIMIT];
  int pos;        // Current position in this directory
  MSGPACK_DEFINE_ARRAY(pathname, pos);
};

struct dirent {
  ino_t d_ino;
  unsigned char d_type;
  char d_name[FILENAME_LIMIT];
  MSGPACK_DEFINE_ARRAY(d_ino, d_type, d_name);
};

}

#endif