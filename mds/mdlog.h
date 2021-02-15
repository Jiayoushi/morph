#ifndef MORPH_MDS_MDLOG_H
#define MORPH_MDS_MDLOG_H

#include <cstdint>
#include <memory>
#include <common/types.h>
#include <common/nocopy.h>
#include <rpc/client.h>
#include <common/rpc_args.h>
#include <iostream>

namespace morph {

enum Operation {
  CREATE_INODE = 0,
  UPDATE_INODE = 1,
  REMOVE_INODE = 2,
};

struct Log {
  op_t op;
  std::string key;
  std::string data;

  MSGPACK_DEFINE_ARRAY(op, key, data);

  Log() {}

  // TODO: is the use of move correct here?
  Log(Operation o, ino_t ino, type_t type, std::string &&d):
    op(o), data(std::move(d)) {
      key = std::to_string(ino) + "-" + std::to_string(type);
    }
};

struct Handle {
  hid_t id;
  std::vector<Log> logs;
  MSGPACK_DEFINE_ARRAY(id, logs);
};

struct MetadataChangeArgs {
  Handle handle;
  MSGPACK_DEFINE_ARRAY(handle);
};

struct MetadataChangeReply {
  int ret_val;
  MSGPACK_DEFINE_ARRAY(ret_val);
};


/*
 */
class MetadataLog: NoCopy {
 public:
  MetadataLog(const std::string &storage_ip, const unsigned short storage_port);

  void log(std::vector<Log> &&logs);

 private:
  // TODO: write the transactions to the local, then send to the remote storage
  //       Q: we can probably put everything in the memory until OFM? Then we gotta release some memory, and read
  //          those released memory from disk when we need to sync with the remote storage.

  // TODO: in the future there should be a list of rpc clients that corresponds to some or all of the 
  // storage servers.
  rpc::client rpc_client;
};

}

#endif