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

FILE(GLOB PROTO_PB_HEADER proto_out/*.pb.h)
FILE(GLOB PROTO_PB_SOURCE proto_out/*.pb.cc)
FILE(GLOB PROTO_GRPC_PB_HEADER proto_out/*.grpc.pb.h)
FILE(GLOB PROTO_GRPC_PB_SOURCE proto_out/*.grpc.pb.cc)

FILE(GLOB COMMON_SRC_FILES src/common/*.cc)
FILE(GLOB MDS_SRC_FILES src/mds/*.cc)
FILE(GLOB OS_SRC_FILES src/os/*.cc)
FILE(GLOB MONITOR_SRC_FILES src/monitor/*.cc)

add_executable(namespace_test
  ${COMMON_SRC_FILES}
  ${MDS_SRC_FILES}
  src/tests/mds/namespace_test.cc)
add_executable(monitor_test
  ${COMMON_SRC_FILES}
  ${MONITOR_SRC_FILES}
  src/tests/monitor/monitor_test.cc)
add_executable(block_store_test
  src/common/filename.cc
  src/common/env_posix.cc
  src/common/status.cc
  src/common/network.cc
  src/os/block_allocator.cc
  src/os/block_store.cc
  src/os/buffer.cc
  src/tests/os/block_store_test.cc)
add_executable(object_store_test
  ${COMMON_SRC_FILES}
  ${OS_SRC_FILES}
  src/tests/os/object_store_test.cc)
add_executable(third_party_test
  src/tests/third_party.cc)
add_executable(integration_test
  ${OS_SRC_FILES}
  ${COMMON_SRC_FILES}
  ${MONITOR_SRC_FILES}
  src/tests/integration/integration_test.cc)

add_library(morph_grpc_proto
  ${PROTO_PB_HEADER}
  ${PROTO_PB_SOURCE}
  ${PROTO_GRPC_PB_HEADER}
  ${PROTO_GRPC_PB_SOURCE})


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
target_link_libraries(monitor_test
  gtest
  gRPC::grpc++
  morph_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})
target_link_libraries(object_store_test
  -laio
  gtest
  gRPC::grpc++
  morph_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF}
  /usr/local/lib/librocksdb.a
  /usr/lib/x86_64-linux-gnu/libsnappy.so
  /usr/lib/x86_64-linux-gnu/liblz4.so.1
  /usr/lib/x86_64-linux-gnu/libdl.so
  /usr/lib/x86_64-linux-gnu/libz.so)
target_link_libraries(integration_test
  -laio
  gtest
  gRPC::grpc++
  morph_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF}
  /usr/local/lib/librocksdb.a
  /usr/lib/x86_64-linux-gnu/libsnappy.so
  /usr/lib/x86_64-linux-gnu/liblz4.so.1
  /usr/lib/x86_64-linux-gnu/libdl.so
  /usr/lib/x86_64-linux-gnu/libz.so)
target_link_libraries(block_store_test
  gtest
  -laio)
target_link_libraries(third_party_test
  gtest
  /usr/local/lib/librocksdb.a
  /usr/lib/x86_64-linux-gnu/libsnappy.so
  /usr/lib/x86_64-linux-gnu/liblz4.so.1
  /usr/lib/x86_64-linux-gnu/libdl.so
  /usr/lib/x86_64-linux-gnu/libz.so)
