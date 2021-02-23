#ifndef MORPH_MDS_MDLOG_H
#define MORPH_MDS_MDLOG_H

#include <cstdint>
#include <memory>
#include <common/types.h>
#include <common/nocopy.h>
#include <rpc/client.h>
#include <common/rpc_args.h>
#include <storage/journal.h>
#include <iostream>

namespace morph {

enum inode_operation_t {
  CREATE_INODE = 0,
  UPDATE_INODE = 1,
  REMOVE_INODE = 2,
};

struct MetadataChangeArgs {
  char block[JOURNAL_BLOCK_SIZE];
  MSGPACK_DEFINE_ARRAY(block);
};

struct MetadataChangeReply {
  int ret_val;
  MSGPACK_DEFINE_ARRAY(ret_val);
};


class MetadataLog: NoCopy {
 public:
  MetadataLog(const std::string &storage_ip, const unsigned short storage_port);

  LogHandle *journal_start();
  void journal_end(LogHandle *log_handle);

 private:
  Journal journal;

  // TODO: in the future there should be a list of rpc clients that corresponds to some or all of the 
  // storage servers.
  rpc::client rpc_client;
};

}

#endif