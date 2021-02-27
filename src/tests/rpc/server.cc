#include <string>

#include <grpcpp/grpcpp.h>
#include <proto_out/math.grpc.pb.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using mathtest::MathTest;
using mathtest::Stat;
using mathtest::TestRequest;
using mathtest::TestReply;
using mathtest::MultiplyRequest;
using mathtest::MultiplyReply;

class MathServiceImpl final: public MathTest::Service {
 private:
  Status multiply(ServerContext *context, const MultiplyRequest *request, MultiplyReply *reply) override {
   int a = request->a();
   int b = request->b();

   reply->set_result(a * b);

   return Status::OK;
  }

  Status test(ServerContext *context, const TestRequest *request, TestReply *reply) override {
    const std::string &pathname = request->pathname();
    const Stat &stat = request->stat();

    std::cout << "RECEIVED PATHNAME: " << pathname << std::endl;
    std::cout << "STAT: " << stat.a() << " " << stat.b() << std::endl;
    reply->set_result(pathname.size());
    return Status::OK;
  }
};

void run() {
  std::string address("0.0.0.0:5000");
  MathServiceImpl service;

  ServerBuilder builder;
  
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on port: " << address << std::endl;

  server->Wait();
}

int main(int argc, char *argv[]) {
  run();

  return 0;
}
