#include <string>

#include <grpcpp/grpcpp.h>
#include <proto_out/math.grpc.pb.h>


using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using mathtest::MathTest;
using mathtest::Stat;
using mathtest::TestRequest;
using mathtest::TestReply;
using mathtest::MultiplyRequest;
using mathtest::MultiplyReply;

class MathTestClient {
 public:
  MathTestClient(std::shared_ptr<Channel> channel):
    stub(MathTest::NewStub(channel)) {}
  
  int multiply(int a, int b) {
    MultiplyRequest request;
    MultiplyReply reply;
    ClientContext context;
    Status status;

    request.set_a(a);
    request.set_b(b);

    status = stub->multiply(&context, request, &reply);
  
    if (status.ok()) {
      return reply.result();
    } else {
      std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      return -1;
    }
  }

  int test(const char *filename) {
    TestRequest request;
    TestReply reply;
    ClientContext context;
    Status status;
    Stat *stat;

    stat = new Stat();
    stat->set_a(0);
    stat->set_b(1);

    request.set_pathname(filename);
    request.set_allocated_stat(stat);

    status = stub->test(&context, request, &reply);

    if (status.ok()) {
      return reply.result();
    } else {
      std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      return -1;
    }
  }


 private:
  std::unique_ptr<MathTest::Stub> stub;
};

void run() {
  std::string address("0.0.0.0:5000");
  MathTestClient client(
    grpc::CreateChannel(address, grpc::InsecureChannelCredentials())
  );

  int response;
  int a = 5;
  int b = 10;
  
  response = client.multiply(a, b);
  assert(response == 50);

  response = client.test("NICE DIRECTORY");
  assert(response == strlen("NICE DIRECTORY"));
}

int main(int argc, char *argv[]) {
  run();
  return 0;
}
