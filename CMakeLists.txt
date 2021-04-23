cmake_minimum_required(VERSION 3.15)
project(morph)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
set(GRPC_PLUGIN "grpc_cpp_pluin")
set(_GRPC_GRPCPP grpc++)
set(_REFLECTION grpc++_reflection)
set(_PROTOBUF_LIBPROTOBUF libprotobuf)

find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)
find_package(gRPC REQUIRED)

include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

FetchContent_Declare(
  gRPC
  GIT_REPOSITORY https://github.com.cnpmjs.org/grpc/grpc
  GIT_TAG        v1.35.0
)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com.cnpmjs.org/google/googletest.git
  GIT_TAG        release-1.8.0
)

FetchContent_MakeAvailable(gRPC)
FetchContent_MakeAvailable(googletest)

include_directories(/usr/local/include)
include_directories(src)
include_directories(.)

add_executable(namespace_test
  src/mds/namespace.cc
  src/tests/mds/namespace_test.cc)
add_executable(block_store_test
  src/os/block_allocator.cc
  src/os/block_store.cc
  src/os/buffer.cc
  src/tests/os/block_store_test.cc)
add_executable(object_store_test
  src/os/block_allocator.cc
  src/os/block_store.cc
  src/os/buffer.cc
  src/os/kv_store.cc
  src/os/object.cc
  src/os/object_store.cc
  src/tests/os/object_store_test.cc)
add_executable(third_party_test
  src/tests/third_party.cc)

add_library(morph_grpc_proto
  "proto_out/mds.pb.cc"
  "proto_out/mds.pb.h"
  "proto_out/mds.grpc.pb.cc"
  "proto_out/mds.grpc.pb.h")

target_link_libraries(morph_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})
# TODO: It is required to link the _REFLECTION, _GRPC_GRPCPP, _PROTOBUF_LIBPROTOBUF here,
# otherwise there will be memory/initialization-related error emitted when you create a
# message instance. Find out why later.
target_link_libraries(namespace_test
  gtest
  gRPC::grpc++
  morph_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})
target_link_libraries(block_store_test
  gtest
  -laio)
target_link_libraries(object_store_test
  -laio
  gtest
  /usr/local/lib/librocksdb.a
  /usr/lib/x86_64-linux-gnu/libsnappy.so
  /usr/lib/x86_64-linux-gnu/liblz4.so.1
  /usr/lib/x86_64-linux-gnu/libdl.so
  /usr/lib/x86_64-linux-gnu/libz.so)
target_link_libraries(third_party_test
  gtest
  /usr/local/lib/librocksdb.a
  /usr/lib/x86_64-linux-gnu/libsnappy.so
  /usr/lib/x86_64-linux-gnu/liblz4.so.1
  /usr/lib/x86_64-linux-gnu/libdl.so
  /usr/lib/x86_64-linux-gnu/libz.so)
