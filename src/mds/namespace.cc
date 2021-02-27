#include "namespace.h"

#include <sstream>
#include <string>
#include <memory>
#include <iostream>
#include <common/utils.h>

const ino_t FIRST_INODE_NUMBER = 1;

namespace morph {

Namespace::Namespace(std::shared_ptr<grpc::Channel> channel, std::shared_ptr<spdlog::logger> logger):
  next_inode_number(FIRST_INODE_NUMBER),
  journal() {
  std::shared_ptr<Inode> root_inode;
  
  root = allocate_inode<InodeDirectory>(INODE_TYPE::DIRECTORY, 0, 0);
}

Namespace::~Namespace() {}

int Namespace::mkdir(cid_t cid, const char *pathname, mode_t mode) {
  std::shared_ptr<InodeDirectory> parent;
  std::shared_ptr<InodeDirectory> new_dir;
  std::vector<std::string> components;

  components = get_pathname_components(pathname);
  parent = lookup_parent(components);

  if (parent == nullptr) {
    return ENOENT;
  }

  if (parent->find_dentry(components.back().c_str()) != nullptr) {
    return EEXIST;
  }

  new_dir = allocate_inode<InodeDirectory>(INODE_TYPE::DIRECTORY, mode, cid);
  parent->add_dentry(components.back().c_str(), new_dir->ino);

  //LogHandle *handle = journal.start();
  //handle->write_data<InodeDirectory>(CREATE_INODE, form_log_key(new_dir->ino, new_dir->type), *new_dir);
  //handle->write_data<InodeDirectory>(UPDATE_INODE, form_log_key(parent->ino, parent->type), *parent);
  //journal.end(handle);

  return 0;
}

int Namespace::stat(cid_t cid, const char *path, mds_rpc::Stat *stat) {
  std::shared_ptr<Inode> inode;
  std::vector<std::string> components;

  components = get_pathname_components(path);
  inode = lookup(components);
  if (inode == nullptr) {
    return -1;
  }
  stat->set_st_ino(inode->ino);
  stat->set_st_mode(inode->mode);
  stat->set_st_uid(inode->uid);
  return 0;
}


// TODO: check the inode
int Namespace::opendir(const char *pathname) {
  std::vector<std::string> components;

  components = get_pathname_components(pathname);
  if (lookup(components) == nullptr) {
    return -1;
  }
  return 0;
}

int Namespace::rmdir(const char *pathname) {
  std::vector<std::string> components;
  std::shared_ptr<InodeDirectory> parent;
  std::shared_ptr<Inode> dir;
  std::shared_ptr<Dentry> dentry;

  components = get_pathname_components(pathname);
  parent = lookup_parent(components);
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

  std::shared_ptr<InodeDirectory> ptr = std::static_pointer_cast<InodeDirectory>(dir);
  if (!ptr->empty()) {
    return ENOTEMPTY;
  }

  parent->remove_dentry(dentry->ino);
  remove_inode(dentry->ino);

  return 0;
}

int Namespace::readdir(const mds_rpc::DIR *dir, mds_rpc::dirent *dirent) {
  std::vector<std::string> components;
  std::shared_ptr<Inode> ip;
  std::shared_ptr<InodeDirectory> dirp;
  std::shared_ptr<Dentry> dentry;

  components = get_pathname_components(dir->pathname());
  ip = lookup(components);
  if (ip == nullptr) {
    return -1;
  }
  dirp = std::static_pointer_cast<InodeDirectory>(ip);
  dentry = dirp->get_dentry(dir->pos());
  if (dentry == nullptr) {
    return -1;
  }

  ip = get_inode(dentry->ino);
  dirent->set_d_ino(ip->ino);
  dirent->set_d_type(ip->type);
  dirent->set_d_name(dentry->name);

  return 0;
}

std::shared_ptr<Inode> Namespace::get_inode(ino_t ino) {
  auto res = inode_map.find(ino);
  if (res == inode_map.end()) {
    return nullptr;
  }
  return res->second;
}

std::shared_ptr<Inode> Namespace::lookup(const std::vector<std::string> &components) {
  return pathwalk(components);
}

std::shared_ptr<InodeDirectory> Namespace::lookup_parent(const std::vector<std::string> &components) {
  std::shared_ptr<Inode> res;
  res = pathwalk(components, true);
  return res ? std::static_pointer_cast<InodeDirectory>(res) : nullptr;
}

std::shared_ptr<Inode> Namespace::pathwalk(const std::vector<std::string> &components, 
                                           bool stop_at_parent) {
  std::shared_ptr<Inode> parent = root;
  std::shared_ptr<Inode> next = nullptr;
  std::shared_ptr<Dentry> dentry;
  InodeDirectory *dir;

  for (size_t i = 0; i < components.size(); ++i) {
    if (stop_at_parent && i == components.size() - 1) {
      return parent;
    }

    if (parent->type == INODE_TYPE::DIRECTORY) {
      dir = static_cast<InodeDirectory *>(parent.get());
      dentry = dir->find_dentry(components[i].c_str());
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

void Namespace::remove_inode(ino_t ino) {
  inode_map.erase(ino);
}

std::string Namespace::form_log_key(ino_t ino, type_t type) {
  return std::to_string(ino) + "-" + std::to_string(type);
}

}