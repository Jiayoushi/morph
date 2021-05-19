#include "inode_directory.h"

namespace morph {

namespace mds {

Dentry * InodeDirectory::find_child(const char *name) {
  for (Dentry *child: children) {
    if (strcmp(child->name.c_str(), name) == 0) {
      return child;
    }
  }
  return nullptr;
}

void InodeDirectory::remove_child(InodeNumber ino) {
  for (auto iter = children.begin(); iter != children.end(); ++iter) {
    if ((*iter)->ino == ino) {
      delete *iter;
      children.erase(iter);
      return;
    }
  }
  assert(false);
}



} // namespace mds

} // namespace morph