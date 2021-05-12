#include "namespace.h"

#include <sstream>
#include <string>
#include <memory>
#include <iostream>
#include <common/utils.h>
#include <mutex>
#include <condition_variable>

#include "config.h"
#include "log_reader.h"
#include "log_writer.h"
#include "write_batch.h"
#include "write_batch_internal.h"
#include "common/filename.h"

namespace morph {

namespace mds {

const InodeNumber FIRST_INODE_NUMBER = 1;
const uint64_t FIRST_LOG_FILE_NUMBER = 0;

struct Namespace::Writer {
  explicit Writer(std::mutex *mu):
    batch(nullptr), sync(false), done(false)
  {}

  Status status;
  WriteBatch *batch;
  bool sync;
  bool done;
  std::condition_variable cv;
};

Namespace::Namespace(const std::string &name):
  name(name),
  root(nullptr),
  next_inode_number(FIRST_INODE_NUMBER),
  logged_batch_size(0), logfile_number(FIRST_LOG_FILE_NUMBER), 
  sequence_number(0), log(nullptr), logfile(nullptr), tmp_batch(nullptr) {}

Status Namespace::open(std::shared_ptr<spdlog::logger> logger) {
  Status s;

  // Try to recover if directory exists
  if (file_exists(name.c_str())) {
    s = recover();
    if (!s.is_ok()) {
      return s;
    }
  } else {
    s = create_directory(name.c_str());
    if (!s.is_ok()) {
      return s;
    }
    root = allocate_inode<InodeDirectory>(INODE_TYPE::DIRECTORY, 0, 0);
  }

  WritableFile *lfile;
  s = new_writable_file(log_file_name(name.c_str(), logfile_number), &lfile);
  if (s.is_ok()) {
    logfile = lfile;
    log = new log::Writer(logfile);
  }

  tmp_batch = new WriteBatch();
  logger = logger;

  return s;
}

// If there is any log file, recover from that, otherwise set log number to
// a initial value.
Status Namespace::recover() {
  std::vector<std::string> names;
  Status s;
  
  s = get_children("mds", &names);
  if (!s.is_ok()) {
    return s;
  }

  sort(names.begin(), names.end());

  // Sync the logs to remote ods.
  for (const auto &name: names) {
    s = sync_log_to_oss(name);
    if (!s.is_ok()) {
      return s;
    }
  }

  // TODO: Read all metadata from remote ods.

  return s;
}

Status Namespace::sync_log_to_oss(const std::string &name) {
  SequentialFile *file;
  Status s = new_sequential_file(name, &file);
  if (!s.is_ok()) {
    return s;
  }

  // Read all the records and apply to oss.
  log::Reader reader(file, true);
  std::string scratch;
  Slice record;
  WriteBatch batch;
  while (reader.read_record(&record, &scratch)) {
    if (record.size() < 12) {
      fprintf(stderr, "log record too small");
      assert(false);
    }
    WriteBatchInternal::set_contents(&batch, record);
  }

  // TOOD: Delete this log file

  return s;
}

Namespace::~Namespace() {
  delete tmp_batch;
  delete logfile;
  delete log;

  while (!inode_map.empty()) {
    auto iter = inode_map.begin();
    delete iter->second;
    inode_map.erase(iter);
  }
}

int Namespace::mkdir(uid_t uid, const char *pathname, mode_t mode) {
  InodeDirectory *parent;
  InodeDirectory *new_dir;
  std::vector<std::string> components;

  components = get_pathname_components(pathname);
  parent = get_parent_inode(components);

  if (parent == nullptr) {
    return ENOENT;
  }

  if (parent->find_dentry(components.back().c_str()) != nullptr) {
    return EEXIST;
  }

  new_dir = allocate_inode<InodeDirectory>(INODE_TYPE::DIRECTORY, mode, uid);
  parent->add_dentry(components.back().c_str(), new_dir->ino);
  new_dir->links += 1;
  parent->links += 1;

  std::string new_dir_data = new_dir->serialize();
  std::string parent_data =  parent->serialize();

  WriteBatch batch;
  batch.put(Slice(std::to_string(parent->ino)), Slice(new_dir_data));
  batch.put(Slice(std::to_string(parent->ino)), Slice(parent_data));
  Status s = write_to_log(true, &batch);
  assert(s.is_ok());

  return 0;
}

int Namespace::stat(uid_t uid, const char *path, mds_rpc::FileStat *stat) {
  Inode *inode;
  std::vector<std::string> components;

  components = get_pathname_components(path);
  inode = lookup(components);
  if (inode == nullptr) {
    return -1;
  }

  stat->set_ino(inode->ino);
  stat->set_mode(inode->mode);
  stat->set_uid(inode->uid);
  stat->set_nlink(inode->links);
  return 0;
}


// TODO: check the inode
int Namespace::opendir(uid_t uid, const char *pathname) {
  std::vector<std::string> components;

  components = get_pathname_components(pathname);
  if (lookup(components) == nullptr) {
    return ENOENT;
  }
  return 0;
}

// TODO: what should it even do..?
int closedir() {
  return 0;
}

int Namespace::rmdir(uid_t uid, const char *pathname) {
  std::vector<std::string> components;
  InodeDirectory *parent;
  Inode *dir;
  Dentry *dentry;

  components = get_pathname_components(pathname);
  parent = get_parent_inode(components);
  if (parent == nullptr) {
    return ENOENT;
  }
  dentry = parent->find_dentry(components.back().c_str());
  if (dentry == nullptr) {
    return ENOENT;
  }

  dir = get_inode(dentry->ino);
  if (dir->type != INODE_TYPE::DIRECTORY) {
    return ENOTDIR;
  }

  InodeDirectory *ptr = dynamic_cast<InodeDirectory *>(dir);
  assert(ptr != nullptr);
  if (!ptr->empty()) {
    return ENOTEMPTY;
  }

  parent->links -= 1;
  dir->links -= 1;
  InodeNumber ino = dentry->ino;
  parent->remove_dentry(ino);
  remove_inode(ino);

  return 0;
}

int Namespace::readdir(uid_t uid, const mds_rpc::DirRead *dir, 
    mds_rpc::DirEntry *dirent) {
  std::vector<std::string> components;
  Inode *ip;
  InodeDirectory *dirp;
  Dentry *dentry;

  components = get_pathname_components(dir->pathname());
  ip = lookup(components);
  if (ip == nullptr) {
    return -1;
  }
  dirp = dynamic_cast<InodeDirectory *>(ip);
  assert(dirp != nullptr);
  dentry = dirp->get_dentry(dir->pos());
  if (dentry == nullptr) {
    return -1;
  }

  ip = get_inode(dentry->ino);
  dirent->set_ino(ip->ino);
  dirent->set_type(ip->type);
  dirent->set_name(dentry->name);

  return 0;
}

Inode * Namespace::get_inode(InodeNumber ino) {
  auto res = inode_map.find(ino);
  if (res == inode_map.end()) {
    return nullptr;
  }
  return res->second;
}

Inode * Namespace::lookup(const std::vector<std::string> &components) {
  return pathwalk(components);
}

InodeDirectory * Namespace::get_parent_inode(
    const std::vector<std::string> &components) {
  Inode *res;
  res = pathwalk(components, true);
  if (res == nullptr) {
    return nullptr;
  }

  InodeDirectory *parent = dynamic_cast<InodeDirectory *>(res);
  assert(parent != nullptr);
  return parent;
}

Inode * Namespace::pathwalk(const std::vector<std::string> &components, 
    bool stop_at_parent) {
  Inode *parent = root;
  Inode *next = nullptr;

  for (size_t i = 0; i < components.size(); ++i) {
    if (stop_at_parent && i == components.size() - 1) {
      return parent;
    }

    if (parent->type == INODE_TYPE::DIRECTORY) {
      InodeDirectory *dir = dynamic_cast<InodeDirectory *>(parent);
      assert(dir != nullptr);

      Dentry *dentry = dir->find_dentry(components[i].c_str());
      if (dentry == nullptr) {
        return nullptr;
      }
      next = get_inode(dentry->ino);
    }
    if (next == nullptr) {
      return nullptr;
    }

    parent = next;
  }

  return !components.empty() ? next : root;
}

std::vector<std::string> Namespace::get_pathname_components(const std::string &pathname) {
  std::vector<std::string> res;
  std::stringstream ss(pathname);
  std::string token;

  while (std::getline(ss, token, '/')) {
    if (!token.empty()) {
      res.push_back(token);
    }
  }
  
  return res;
}

void Namespace::remove_inode(InodeNumber ino) {
  auto iter = inode_map.find(ino);
  assert(iter != inode_map.end());
  assert(iter->second->links == 0);
  delete iter->second;
  inode_map.erase(iter);
}

Status Namespace::write_to_log(bool sync, WriteBatch *updates) {
  assert(updates != nullptr);

  Writer w(&mutex);
  w.batch = updates;
  w.sync = sync;
  w.done = false;

  std::unique_lock<std::mutex> lock(mutex);
  writers.push_back(&w);
  w.cv.wait(lock, [&w, this](){
    return w.done || &w == this->writers.front();
  });
  if (w.done) {
    return w.status;
  }
  
  Status status = make_room_for_log_write();
  uint64_t last_sequence = sequence_number;
  Writer *last_writer = &w;
  if (status.is_ok()) {
    WriteBatch *write_batch = build_batch_group(&last_writer);
    WriteBatchInternal::set_sequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::count(write_batch);

    // Add log. We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent logger
    mutex.unlock();
    status = log->add_record(WriteBatchInternal::contents(write_batch));
    bool sync_error = false;
    if (status.is_ok() && sync) {
      status = logfile->sync();
      if (!status.is_ok()) {
        sync_error = true;
      }
      logged_batch_size += write_batch->approximate_size();
    }
    mutex.lock();
    if (sync_error) {
      // The state of the log file is indeterminate: the log record we
      // just added may or may not show up when the DB is re-opened.
      // So we force the DB into a mode where all future writes fail.
      record_background_error(status);
    }
    if (write_batch == tmp_batch) {
      tmp_batch->clear();
    }
    sequence_number = last_sequence;
  }

  while (true) {
    Writer *ready = writers.front();
    writers.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.notify_all();
    }
    if (ready == last_writer) {
      break;
    }
  }

  // Notify new head of write queue
  if (!writers.empty()) {
    writers.front()->cv.notify_all();
  }

  return status;
}

Status Namespace::make_room_for_log_write() {
  assert(!writers.empty());
  Status s;

  if (logged_batch_size > config::MAX_LOG_FILE_SIZE) {
    uint64_t new_log_number = logfile_number + 1;
    WritableFile *lfile;
    s = new_writable_file(log_file_name("mds", new_log_number), &lfile);
    if (!s.is_ok()) {
      return s;
    }
 
    delete logfile;
    delete log;
    logfile = lfile;
    logfile_number = new_log_number;
    log = new log::Writer(lfile);
    logged_batch_size = 0;
  }

  return s;
}

void Namespace::record_background_error(const Status &s) {
  if (bg_error.is_ok()) {
    bg_error = s;
    background_work_finished_signal.notify_all();
  }
}

WriteBatch * Namespace::build_batch_group(Writer **last_writer) {
  assert(!writers.empty());
  Writer *first = writers.front();
  WriteBatch *result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::byte_size(first->batch);

  // Allow the group to group up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer *>::iterator iter = writers.begin();
  ++iter;
  for (; iter != writers.end(); ++iter) {
    Writer *w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::byte_size(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch;
        assert(WriteBatchInternal::count(result) == 0);
        WriteBatchInternal::append(result, w->batch);
      }
    }
    *last_writer = w;
  }
  return result;
}

} // namespace mds

} // namespace morph