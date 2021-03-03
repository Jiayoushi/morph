#ifndef MORPH_TYPES_H
#define MORPH_TYPES_H

#include <sys/types.h>
#include <rpc/server.h>
#include <common/config.h>

namespace morph {

typedef uint64_t cid_t;        // Client's id
typedef uint8_t  type_t;
typedef uint32_t op_t;
typedef uint64_t hid_t;        // Handle id
typedef uint64_t rid_t;        // Request id
typedef uint32_t bno_t;        // Block number

typedef std::string LogBuffer;

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