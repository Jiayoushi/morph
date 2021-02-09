#include "namenode.h"

#include <sstream>
#include <string>
#include <memory>
#include <iostream>
#include <common/utils.h>
#include <mds/mdlog.h>

const ino_t FIRST_INODE_NUMBER = 1;

namespace morph {

NameNode::NameNode(const std::string &storage_ip, const unsigned short storage_port):
  next_inode_number(FIRST_INODE_NUMBER),
  mdlog(storage_ip, storage_port) {
  std::shared_ptr<Inode> root_inode;
  
  root = allocate_inode<InodeDirectory>(INODE_TYPE::DIRECTORY, 0, 0);
}

NameNode::~NameNode() {}

int NameNode::mkdir(cid_t cid, const char *pathname, mode_t mode) {
  std::shared_ptr<InodeDirectory> parent;
  std::shared_ptr<InodeDirectory> new_dir;
  std::vector<std::string> components;

  components = get_pathname_components(pathname);
  parent = lookup_parent(components);

  if (parent == nullptr) {
    return ENOENT;
  }

  if (parent->find_dentry(components.back().c_str(), nullptr) == 0) {
    return EEXIST;
  }

  new_dir = allocate_inode<InodeDirectory>(INODE_TYPE::DIRECTORY, mode, cid);
  parent->add_dentry(components.back().c_str(), new_dir->ino);

  // Log meta changes of the new directory and the parent's metadata
  // note that parent's children are metadata in morph, not stored as data
  std::vector<Log> logs;
  logs.emplace_back(CREATE_INODE, new_dir->ino, new_dir->type, std::move(serialize<InodeDirectory>(*new_dir)));
  logs.emplace_back(UPDATE_INODE, parent->ino, parent->type, std::move(serialize<InodeDirectory>(*parent)));
  mdlog.log(std::move(logs));

  return 0;
}

int NameNode::stat(cid_t cid, const char *path, struct morph::stat *buf) {
  std::shared_ptr<Inode> dentry;
  std::vector<std::string> components;

  components = get_pathname_components(path);
  dentry = lookup(components);
  if (dentry == nullptr) {
    return -1;
  }
  buf->st_ino = dentry->ino;
  buf->st_mode = dentry->mode;
  buf->st_uid = dentry->uid;
  return 0;
}


// TODO: check the inode
int NameNode::opendir(const char *pathname) {
  std::vector<std::string> components;

  components = get_pathname_components(pathname);
  if (lookup(components) == nullptr) {
    return -1;
  }
  return 0;
}

int NameNode::readdir(const DIR *dir, dirent *dirent) {
  std::vector<std::string> components;
  std::shared_ptr<Inode> ip;
  std::shared_ptr<InodeDirectory> dirp;
  Dentry dentry;

  components = get_pathname_components(dir->pathname);
  ip = lookup(components);
  if (ip == nullptr) {
    return -1;
  }
  dirp = std::static_pointer_cast<InodeDirectory>(ip);
  if (dirp->get_dentry(dir->pos, &dentry) < 0) {
    return -1;
  }

  ip = get_inode(dentry.ino);
  dirent->d_ino = ip->ino;
  dirent->d_type = ip->type;
  strcpy(dirent->d_name, dentry.name);

  return 0;
}

std::shared_ptr<Inode> NameNode::get_inode(ino_t ino) {
  auto res = inode_map.find(ino);
  if (res == inode_map.end()) {
    return nullptr;
  }
  return res->second;
}

std::shared_ptr<Inode> NameNode::lookup(const std::vector<std::string> &components) {
  return pathwalk(components);
}

std::shared_ptr<InodeDirectory> NameNode::lookup_parent(const std::vector<std::string> &components) {
  std::shared_ptr<Inode> res;
  res = pathwalk(components, true);
  return res ? std::static_pointer_cast<InodeDirectory>(res) : nullptr;
}

std::shared_ptr<Inode> NameNode::pathwalk(const std::vector<std::string> &components, 
                                           bool stop_at_parent) {
  std::shared_ptr<Inode> parent = root;
  std::shared_ptr<Inode> next = nullptr;
  InodeDirectory *dir;
  Dentry dentry;

  for (size_t i = 0; i < components.size(); ++i) {
    if (stop_at_parent && i == components.size() - 1) {
      return parent;
    }

    if (parent->type == INODE_TYPE::DIRECTORY) {
      dir = static_cast<InodeDirectory *>(parent.get());
      if (dir->find_dentry(components[i].c_str(), &dentry) < 0) {
        return nullptr;
      }
      next = get_inode(dentry.ino);
    }
    if (next == nullptr) {
      return nullptr;
    }

    parent = next;
  }

  return !components.empty() ? next : root;
}

std::vector<std::string> NameNode::get_pathname_components(const std::string &pathname) {
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


}