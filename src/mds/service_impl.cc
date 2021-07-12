#include "service_impl.h"

#include <spdlog/fmt/bundled/printf.h>

#include "common/filename.h"
#include "common/consistent_hash.h"
#include "log_reader.h"
#include "write_batch_internal.h"

namespace morph {

namespace mds {

MetadataServiceImpl::MetadataServiceImpl(const std::string &name,
                                         const NetworkAddress &addr,
                                         const Config &monitor_config,
                                         const std::shared_ptr<spdlog::logger> lg,
                                         const bool need_recover):
    this_name(name), this_addr(addr), logger(lg), name_space(name, need_recover) {
  // Copy the monitor cluster config into this data structure
  assert(monitor_config.infos.size() == 1);
  for (const Info &info: monitor_config.infos) {
    monitor_cluster.add_instance(info.name, info.addr);
  }

  // TODO: right now there is only one monitor
  // Connect to the primary monitor
  primary_monitor = monitor_cluster.get_instance(
    monitor_config.infos.front().name);

  register_mds_to_monitor();

  update_oss_cluster();

  if (need_recover) {
    recover();
  }
}

// If there is any log file, recover from that, otherwise set log number to
// a initial value.
Status MetadataServiceImpl::recover() {
  std::vector<std::string> filenames;
  Status s;
  
  s = get_children(this_name, &filenames);
  if (!s.is_ok()) {
    return s;
  }

  sort(filenames.begin(), filenames.end());

  // Sync the logs to remote ods.
  for (const auto &filename: filenames) {
    if (get_filename_suffix(filename) == "log") {
      std::string full_path = this_name + "/" + filename;
      s = sync_log_to_oss(full_path);
      if (!s.is_ok()) {
        return s;
      }
      //assert(unlink(full_path.c_str()).is_ok());
      //assert(!file_exists(full_path.c_str()));
    }
  }

  // Restore the namespace from remote oss
  name_space.recover(
    [this](const std::string &name) {
      return get_inode_from_oss(name);
  });

  return s;
}

std::string MetadataServiceImpl::get_inode_from_oss(const std::string &name) {
  using namespace oss_rpc;

  GetMetadataRequest request;
  GetMetadataReply reply;
  grpc::ClientContext ctx;
  auto group = assign_group(oss_cluster.get_cluster_names(), name);
  auto oss_instance = oss_cluster.get_instance(*group.begin());

  logger->info(fmt::sprintf("ask oss[%s] for object %s metadata [%s]", 
               oss_instance->name.c_str(), name.c_str(), "inode"));

  request.set_object_name(name);
  request.set_attribute("inode");
  auto s = oss_instance->stub->get_metadata(&ctx, request, &reply);
  assert(s.ok());

  logger->info(fmt::sprintf(
    "get_inode_from_oss: returned status code: %d", reply.ret_val()));

  assert(reply.ret_val() == 0);
  return reply.value();
}

Status MetadataServiceImpl::sync_log_to_oss(const std::string &filename) {
  SequentialFile *file;
  Status status = new_sequential_file(filename, &file);
  if (!status.is_ok()) {
    return status;
  }

  logger->info(fmt::sprintf(
    "sync log to oss: filename[%s] oss_cluster[%d] started", 
    filename.c_str(), oss_cluster.get_version()));

  // Read all the records and apply to oss.
  log::Reader reader(file, true);
  std::string scratch;
  Slice record;
  WriteBatch batch;
  OpVector ops;

  while (reader.read_record(&record, &scratch)) {
    if (record.size() < 12) {
      fprintf(stderr, "log record too small");
      assert(false);
    }
    WriteBatchInternal::set_contents(&batch, record);

    ops.clear();
    status = WriteBatchInternal::insert_into(&batch, &ops);
    assert(status.is_ok());
    assert(!ops.empty());

    for (const Operation &op: ops) {
      int type = std::get<0>(op);
      assert(type == 0 || type == 1);

      if (type == 0) {
        oss_put(std::get<1>(op), std::get<2>(op));
      } else {
        assert(false && "NOT IMPLEMENTED YET");
        oss_del(std::get<1>(op), std::get<2>(op));
      }
    }
  }

  logger->info(fmt::sprintf(
    "sync log to oss: filename[%s] oss_cluster[%d] completed", 
    filename.c_str(), oss_cluster.get_version()));

  delete file;
  return status;
}

void MetadataServiceImpl::oss_put(const Slice &key, const Slice &value) {
  std::string key_string = key.to_string();
  std::string value_string = value.to_string();
  std::vector<std::string> group = 
    assign_group(oss_cluster.get_cluster_names(), key_string);
  oss_rpc::PutMetadataRequest request;
  oss_rpc::PutMetadataReply reply;
  grpc::ClientContext ctx;

  request.set_object_name(key_string);
  request.set_attribute("inode");
  request.set_create_object(true);
  request.set_value(value_string);

  auto oss_instance = oss_cluster.get_instance(*group.begin());
  logger->info(fmt::sprintf(
    "ask oss[%s] to put object[%s] metadata[inode]",
    oss_instance->name.c_str(), key_string.c_str()));

  auto status = oss_instance->stub->put_metadata(&ctx, request, &reply);
  assert(status.ok());

  if (reply.ret_val() != 0) {
    logger->info(fmt::sprintf("failed to put metadata at oss[%s]. Oss error code[%d]",
      oss_instance->name.c_str(), reply.ret_val()));
    assert(false);
  }
}

void MetadataServiceImpl::oss_del(const Slice &key, const Slice &value) {
  
}

grpc::Status MetadataServiceImpl::update_oss_cluster(ServerContext *context,
                                         const UpdateOssClusterRequest *request,
                                         UpdateOssClusterReply *reply) {
  int ret_val = 0;
  
  std::lock_guard<std::mutex> lock(oss_cluster_mutex);

  logger->info(fmt::sprintf(
    "update_oss_cluster called. Current version %d Request version %d", 
    oss_cluster.get_version(), request->version()));

  if (request->version() > oss_cluster.get_version()) {
    std::unordered_map<std::string, NetworkAddress> m;
    for (auto p = request->info().cbegin(); p != request->info().cend(); ++p) {
      m[p->name()] = p->addr();
    }
    oss_cluster.update_cluster(request->version(), m);
  }

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

void MetadataServiceImpl::update_oss_cluster() {
  using namespace monitor_rpc;

  GetOssClusterRequest req;
  GetOssClusterReply reply;
  grpc::ClientContext ctx;

  logger->info(fmt::sprintf("ask monitor for latest oss cluster, current version %d", 
    oss_cluster.get_version()));

  req.set_requester(this_name.c_str());
  req.set_version(oss_cluster.get_version());
  auto s = primary_monitor->stub->get_oss_cluster(&ctx, req, &reply);
  assert(s.ok());
  assert(reply.ret_val() == 0);

  logger->info(fmt::sprintf("oss cluster got from monitor, version %d", 
    reply.version()));

  if (reply.version() == oss_cluster.get_version()) {
    return;
  } else if (reply.version() < oss_cluster.get_version()) {
    assert(false);
  }

  std::lock_guard<std::mutex> lock(oss_cluster_mutex);

  for (auto p = reply.info().cbegin(); p != reply.info().cend(); ++p) {
    oss_cluster.add_instance(p->name(), p->addr());
  }

  logger->info(fmt::sprintf("oss cluster updated, latest version %d", 
    oss_cluster.get_version()));
}


grpc::Status MetadataServiceImpl::mkdir(ServerContext *context, 
    const MkdirRequest *request, MkdirReply *reply) {
  int ret_val;

  logger->debug(fmt::sprintf("mkdir invoked pathname[%s] rid[%d]", 
    request->pathname(), request->rid()));

  ret_val = name_space.mkdir(request->uid(), 
    request->pathname().c_str(), request->mode());

  reply->set_ret_val(ret_val); 
  logger->debug(fmt::sprintf("mkdir pathname[%s] rid[%d] success. Return %d", 
    request->pathname(), request->rid(), reply->ret_val()));
  return grpc::Status::OK;
}

grpc::Status MetadataServiceImpl::opendir(ServerContext *context, 
                                          const OpendirRequest *request, 
                                          OpendirReply *reply) {
  int ret_val;

  ret_val = name_space.opendir(request->uid(), 
    request->pathname().c_str());

  reply->set_ret_val(ret_val); 
  return grpc::Status::OK;
}

grpc::Status MetadataServiceImpl::rmdir(ServerContext *context, 
                                        const RmdirRequest *request, 
                                        RmdirReply *reply) {
  int ret_val;

  ret_val = name_space.rmdir(request->uid(), 
    request->pathname().c_str());

  reply->set_ret_val(ret_val); 
  return grpc::Status::OK;
}

grpc::Status MetadataServiceImpl::stat(ServerContext *context, 
                                       const StatRequest *request, 
                                       StatReply *reply) {
  int ret_val;
  FileStat *stat = new mds_rpc::FileStat();

  ret_val = name_space.stat(request->uid(), 
    request->pathname().c_str(), stat);

  reply->set_ret_val(ret_val);
  reply->set_allocated_stat(stat);

  return grpc::Status::OK;
}

grpc::Status MetadataServiceImpl::readdir(ServerContext *context, 
                                          const ReaddirRequest *request, 
                                          ReaddirReply *reply) {
  int ret_val;
  DirEntry *dirent = new DirEntry();

  ret_val = name_space.readdir(request->uid(), &request->dir(), 
    dirent);

  reply->set_ret_val(ret_val);
  reply->set_allocated_entry(dirent);
  return grpc::Status::OK;
}

void MetadataServiceImpl::register_mds_to_monitor() {
  monitor_rpc::AddMdsRequest request;
  monitor_rpc::AddMdsReply reply;

  grpc::ClientContext context;
  grpc::Status status;  
  monitor_rpc::MdsInfo *info = new monitor_rpc::MdsInfo();

  info->set_name(this_name);
  info->set_addr(this_addr);
  request.set_allocated_info(info);
  status = primary_monitor->stub->add_mds(&context, request, &reply);

  if (!status.ok()) {
    std::cerr << status.error_code() << ": " << status.error_message()
              << std::endl;
    assert(false);
  }

  if (reply.ret_val() == S_SUCCESS || S_EXISTS) {
    return;
  }

  logger->info(fmt::sprintf("mds failed to connect to monitor: error code %d\n", reply.ret_val()));
  assert(false);
}

} // namespace mds
} // namespace morph