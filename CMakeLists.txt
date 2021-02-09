cmake_minimum_required(VERSION 3.5)
project(morph)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
include_directories(/usr/local/include)
include_directories(.)


file(GLOB SOURCES 
  "mds/*.cc"
  "client/*.cc"
  "tests/*.cc"
  "storage/*.cc")

add_executable(test
  ${SOURCES})

target_link_libraries(test
  Threads::Threads
  /usr/local/lib/libspdlog.a
  /usr/local/lib/librpc.a
  /usr/local/lib/librocksdb.a
  /usr/lib/x86_64-linux-gnu/libsnappy.so
  /usr/lib/x86_64-linux-gnu/liblz4.so.1
  /usr/lib/x86_64-linux-gnu/libdl.so
  /usr/lib/x86_64-linux-gnu/libz.so)
