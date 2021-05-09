set -e

out=proto_out

# Create proto directory if not alreayd exists
if [ ! -d $out ]
then
  mkdir $out
fi

# Generate proto files
protoc src/tests/rpc/math.proto \
       -I src/tests/rpc         \
       --grpc_out=$out          \
       --cpp_out=$out           \
       --plugin=protoc-gen-grpc=/home/jyshi/.local/bin/grpc_cpp_plugin

protoc src/protos/mds.proto \
       -I src/protos       \
       --grpc_out=$out     \
       --cpp_out=$out      \
       --plugin=protoc-gen-grpc=/home/jyshi/.local/bin/grpc_cpp_plugin

protoc src/protos/oss.proto \
       -I src/protos       \
       --grpc_out=$out     \
       --cpp_out=$out      \
       --plugin=protoc-gen-grpc=/home/jyshi/.local/bin/grpc_cpp_plugin

protoc src/protos/monitor.proto \
       -I src/protos       \
       --grpc_out=$out     \
       --cpp_out=$out      \
       --plugin=protoc-gen-grpc=/home/jyshi/.local/bin/grpc_cpp_plugin
