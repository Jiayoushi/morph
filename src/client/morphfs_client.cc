#include <client/morphfs_client.h>

#include <string>
#include <iostream>
#include <common/types.h>
#include <spdlog/fmt/bundled/printf.h>
#include <grpcpp/grpcpp.h>

namespace morph {

using grpc::ClientContext;

int error_code;

MorphFsClient::MorphFsClient(const cid_t cid, const std::shared_ptr<Channel> channel):
  mds_stub(MdsService::NewStub(channel)),
  cid(cid),
  rid(0) {

  try {
    std::string filepath = LOGGING_DIRECTORY + "/client-log-" + std::to_string(cid) + ".txt";
    logger = spdlog::basic_logger_mt("client_logger_" + std::to_string(cid), filepath, true);
    logger->set_level(LOGGING_LEVEL);
    logger->flush_on(FLUSH_LEVEL);
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "client Log init failed: " << ex.what() << std::endl;
    exit(EXIT_FAILURE);
  }

  logger->debug("logger initialized");
}

int MorphFsClient::mkdir(const char *pathname, mode_t mode) {
  mds_rpc::MkdirRequest request;
  mds_rpc::MkdirReply reply;
  ClientContext context;
  grpc::Status status;
  uint64_t this_rid = rid++;

  logger->debug(fmt::sprintf("mkdir on pathname[%s] rid[%d]",
                pathname, this_rid));

  request.set_cid(cid);
  request.set_rid(this_rid);
  request.set_mode(mode);
  request.set_pathname(pathname);

  status = mds_stub->mkdir(&context, request, &reply);

  logger->debug(fmt::sprintf("mkdir on pathname[%s] rid[%d] returned [%d] [%s]",
                pathname, this_rid, status.error_code(), status.error_message()));

  if (!status.ok()) {
    return -1;
  }
  if (reply.ret_val() == 0) {
    return 0;
  }
  error_code = reply.ret_val();
  return -1;
}

DIR *MorphFsClient::opendir(const char *pathname) {
  mds_rpc::OpendirRequest request;
  mds_rpc::OpendirReply reply;
  ClientContext context;
  grpc::Status status;
  DIR *dir;
  uint64_t this_rid = rid++;

  request.set_cid(cid);
  request.set_rid(this_rid);
  request.set_pathname(pathname);

  status = mds_stub->opendir(&context, request, &reply);



  if (!status.ok()) {
    return nullptr;
  }

  if (reply.ret_val() == 0) {
    dir = new DIR();
    strcpy(dir->pathname, pathname);
    dir->pos = 0;
    return dir;
  }

  error_code = reply.ret_val();
  return nullptr;
}

int MorphFsClient::rmdir(const char *pathname) {
  mds_rpc::RmdirRequest request;
  mds_rpc::RmdirReply reply;
  ClientContext context;
  grpc::Status status;
  uint64_t this_rid = rid++;

  request.set_cid(cid);
  request.set_rid(this_rid);
  request.set_pathname(pathname);

  status = mds_stub->rmdir(&context, request, &reply);

  if (!status.ok()) {
    return -1;
  }
  if (reply.ret_val() == 0) {
    return 0;
  }
  error_code = reply.ret_val();
  return -1;
}

int MorphFsClient::stat(const char *pathname, morph::stat *buf) {
  mds_rpc::StatRequest request;
  mds_rpc::StatReply reply;
  ClientContext context;
  grpc::Status status;
  uint64_t this_rid = rid++;

  request.set_cid(cid);
  request.set_rid(this_rid);
  request.set_pathname(pathname);
  
  status = mds_stub->stat(&context, request, &reply);

  if (!status.ok()) {
    return -1;
  }

  if (reply.ret_val() == 0) {
    buf->st_ino = reply.stat().st_ino();
    buf->st_mode = reply.stat().st_mode();
    buf->st_uid = reply.stat().st_uid();
    return 0;
  }
  error_code = reply.ret_val();
  return -1;
}


dirent *MorphFsClient::readdir(morph::DIR *dir) {
  mds_rpc::ReaddirRequest request;
  mds_rpc::ReaddirReply reply;
  mds_rpc::DIR *request_dir = new mds_rpc::DIR();
  morph::dirent *dirent;
  ClientContext context;
  grpc::Status status;
  uint64_t this_rid = rid++;

  request_dir->set_pathname(dir->pathname);
  request_dir->set_pos(dir->pos);

  request.set_cid(cid);
  request.set_rid(this_rid);
  request.set_allocated_dir(request_dir);

  status = mds_stub->readdir(&context, request, &reply);

  if (!status.ok()) {
    return nullptr;
  }

  if (reply.ret_val() == 0) {
    ++dir->pos;
    // TODO: this should be statically allocated
    dirent = new morph::dirent(); 
    dirent->d_ino = reply.dirent().d_ino();
    strcpy(dirent->d_name, reply.dirent().d_name().c_str());
    dirent->d_type = reply.dirent().d_type();
    return dirent;
  }

  error_code = reply.ret_val();
  return nullptr;
}

void MorphFsClient::open() {

}

void MorphFsClient::pread() {

}

void MorphFsClient::pwrite() {

}

void MorphFsClient::unlink() {

}

}