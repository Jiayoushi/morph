#ifndef MORPH_RPC_ARGS_H
#define MORPH_RPC_ARGS_H

#include <common/types.h>

namespace morph {

struct MkdirArgs {
  cid_t cid;
  char pathname[PATHNAME_LIMIT];
  mode_t mode;
  MSGPACK_DEFINE_ARRAY(cid, pathname, mode);
};

struct MkdirReply {
  int ret_val;
  MSGPACK_DEFINE_ARRAY(ret_val);
};

struct StatArgs {
  cid_t cid;
  char path[PATHNAME_LIMIT];
  MSGPACK_DEFINE_ARRAY(cid, path);
};

struct StatReply {
  int ret_val;
  morph::stat stat;
  MSGPACK_DEFINE_ARRAY(ret_val, stat);
};

struct OpendirArgs {
  cid_t cid;
  char pathname[PATHNAME_LIMIT];
  MSGPACK_DEFINE_ARRAY(cid, pathname);
};

struct OpendirReply {
  int ret_val;
  MSGPACK_DEFINE_ARRAY(ret_val);
};

struct ReaddirArgs {
  cid_t cid;
  morph::DIR dir;
  MSGPACK_DEFINE_ARRAY(cid, dir);
};

struct ReaddirReply {
  int ret_val;
  morph::dirent dirent;
  MSGPACK_DEFINE_ARRAY(ret_val, dirent);
};

struct RmdirArgs {
  cid_t cid;
  char pathname[PATHNAME_LIMIT];
  MSGPACK_DEFINE_ARRAY(cid, pathname);
};

struct RmdirReply {
  int ret_val;
  MSGPACK_DEFINE_ARRAY(ret_val);
};






}

#endif