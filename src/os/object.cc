#include <os/object.h>


namespace morph {

bool Object::search_extent(off_t file_off, Extent *ext) {
  std::vector<Extent> res;
  EXTENT_TREE_ITER iter;
  lbn_t start, end;
  
  iter = extent_tree.lower_bound(file_off);

  if (iter == extent_tree.end()) {
    if (ext) {
      ext->off_start = INVALID_EXTENT;
    }
    return false;
  }

  if (ext) {
    *ext = iter->second;
  }

  return file_off >= iter->second.off_start 
    && file_off <= iter->second.off_end;
}



/*
 *  [-------delete_range-------] 
 * [ ]  [ ]   [    ]  [ ]  [    ]     // extents
 */
std::vector<std::pair<lbn_t, uint32_t>> Object::delete_extent_range(
    off_t del_start, off_t del_end) {
  std::map<off_t, Extent>::iterator iter;
  Extent ext;
  off_t mutual_start, mutual_end;
  uint32_t len;
  std::vector<std::pair<lbn_t, uint32_t>> remove_blocks;

  //fprintf(stderr, "delete start(%d) end(%d)\n", del_start, del_end);

  while (true) {
    iter = extent_tree.lower_bound(del_start);
    if (iter == extent_tree.end() || iter->second.off_start > del_end) {
      break;
    }

    ext = iter->second;

    // Compute the mutual part
    //fprintf(stderr, "found extent start(%d) end(%d)\n", 
    //  ext.off_start, ext.off_end);

    mutual_start = std::max(del_start, ext.off_start);
    mutual_end = std::min(del_end, ext.off_end);
    len = mutual_end - mutual_start + 1;

    //fprintf(stderr, "mutual_start(%d) mutual_end(%d)\n",
    //  mutual_start, mutual_end);

    assert(len >= 1);

    // delete the mutual part from this extent
    //    [    ]      extent
    // #1 [ ]         mutual part
    // #2  [ ]
    // #3    [ ]
    // #4 [    ]

    // Case #4
    if (mutual_start == ext.off_start && mutual_end == ext.off_end) {
      //fprintf(stderr, "case #4\n");
      extent_tree.erase(iter);
      remove_blocks.emplace_back(ext.lbn, len);
    
    // Case #1
    } else if (mutual_start == ext.off_start) {
      //fprintf(stderr, "case #1\n");
      iter->second.off_start = mutual_end + 1;
      iter->second.lbn += len;
      remove_blocks.emplace_back(ext.lbn, len);

    // Case #3
    } else if (mutual_end == ext.off_end) {
      //fprintf(stderr, "case #3\n");
      extent_tree.erase(iter);
      insert_extent(ext.off_start, mutual_start - 1, ext.lbn);
      remove_blocks.emplace_back(
        ext.lbn + (mutual_start - ext.off_start), len);

    // Case #2
    } else {
      //fprintf(stderr, "case #2\n");
      iter->second.off_start = mutual_end + 1;
      iter->second.lbn += mutual_end - ext.off_start + 1;

      insert_extent(ext.off_start,
        mutual_start - 1,
        ext.lbn);

      remove_blocks.emplace_back(
        ext.lbn + (mutual_start - ext.off_start), len);
    }
  }

  return remove_blocks;
}


}
