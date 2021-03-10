#include <os/object_store.h>

#include <cassert>

namespace morph {

void ObjectStore::PutObject(const std::string &object_name, const uint32_t offset, const std::string &body) {
  std::shared_ptr<Object> object;
  auto f = objects.find(object_name);

  // If the object does not exist, create it
  if (f == objects.end()) {
    object = std::make_shared<Object>();
    assert(offset == 0);
    assert(body.empty());
  } else {
    assert(!body.empty());
    object_write_data(object, offset, body);
  }
}

void ObjectStore::object_write_data(std::shared_ptr<Object> object, const uint32_t offset, const std::string &data) {
  std::vector<Extent> extents;
  std::shared_ptr<BufferGroup> group; 
  lbn_t target_lbn_start = offset / BLOCK_SIZE;
  lbn_t target_lbn_end = (offset + data.size()) / BLOCK_SIZE;

  // File has holes, so it's possible there are more than one extents. Thus these extents may only fill only parts of
  // the range required by this write
  extents = object->search_extent(target_lbn_start, target_lbn_end);

  for (uint32_t off = offset; off != offset + data.size(); ) {

  }

}

}