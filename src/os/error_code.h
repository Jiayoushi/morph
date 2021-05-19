#ifndef MORPH_OS_ERROR_CODE_H
#define MORPH_OS_ERROR_CODE_H

namespace morph {

namespace os {

enum OSS_ERROR_CODE {
  OPERATION_SUCCESS = 0,

  OBJECT_NOT_FOUND = 1,

  // Read parts of an object that has no written content
  NO_CONTENT = 2,

  OBJECT_NAME_INVALID = 3,

  // The requested metadata does not exist
  METADATA_NOT_FOUND = 4,

  // This oss is not the primary oss to store the target object
  NOT_PRIMARY = 5,

  // This oss is not responsible for the target object's storage
  NOT_REPLICATION_GROUP = 6
};

} // namespace os

} // namespace morph


#endif