#include "mdlog.h"

namespace morph {

MetadataLog::MetadataLog(const std::string &storage_ip, const unsigned short storage_port):
  rpc_client(storage_ip, storage_port) {

}

LogHandle * MetadataLog::journal_start() {
  return journal.journal_start();
}

void MetadataLog::journal_end(LogHandle *log_handle) {
  return journal.journal_end(log_handle);
}

}