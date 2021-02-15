#include "mdlog.h"

namespace morph {

MetadataLog::MetadataLog(const std::string &storage_ip, const unsigned short storage_port):
  rpc_client(storage_ip, storage_port) {

}

void MetadataLog::log(std::vector<Log> &&logs) {
  MetadataChangeArgs args;
  MetadataChangeReply reply;

  args.handle.logs = std::move(logs);

  reply = rpc_client.call("metadata_change", args).as<MetadataChangeReply>();
}


}