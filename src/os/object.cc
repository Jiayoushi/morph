#include <os/object.h>

namespace morph {

bool Object::search_extent(lbn_t lbn, Extent &ext) {
  std::vector<Extent> res;
  EXTENT_TREE_ITER iter;
  lbn_t start;
  lbn_t end;
  
  iter = extent_tree.lower_bound(lbn);

  if (iter == extent_tree.end()) {
    ext.start = INVALID_EXTENT;
    return false;
  }

  ext = iter->second;
  return lbn >= iter->second.start && lbn <= iter->second.end;
}


}