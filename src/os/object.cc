#include <os/object.h>

namespace morph {

bool Object::search_extent(lbn_t lbn, Extent *ext) {
  std::vector<Extent> res;
  EXTENT_TREE_ITER iter;
  lbn_t start, end;
  
  iter = extent_tree.lower_bound(lbn);

  if (iter == extent_tree.end()) {
    if (ext) {
      ext->start = INVALID_EXTENT;
    }
    return false;
  }

  if (ext) {
    *ext = iter->second;
  }
  return lbn >= iter->second.start && lbn <= iter->second.end;
}


}