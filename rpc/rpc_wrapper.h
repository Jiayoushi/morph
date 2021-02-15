#ifndef MORPH_RPC_WRAPPER_H
#define MORPH_RPC_WRAPPER_H

#include <iostream>
#include <random>
#include <common/nocopy.h>
#include <rpc/client.h>

namespace morph {


class RpcClient: NoCopy {
 public:
  RpcClient(std::string const &addr, uint16_t port):
    lost_probability(0.0),
    generator(),
    distribution(0.0, 1.0),
    rpc_client(addr, port) {}

  void set_lost_probability(double prob) {
    lost_probability = prob;
  }

  template <typename... Args>
  RPCLIB_MSGPACK::object_handle call(std::string const &func_name,
                                       Args... args);


 private:
  /*
   * Fake network
   */
  double lost_probability;
  std::default_random_engine generator;
  std::uniform_real_distribution<double> distribution;

  int64_t default_timeout;

  rpc::client rpc_client;
};

template <typename... Args>
RPCLIB_MSGPACK::object_handle RpcClient::call(std::string const &func_name,
                                              Args... args) {
  double random = distribution(generator);

  if (random < lost_probability) {
    rpc_client.set_timeout(0);
  } else {
    rpc_client.clear_timeout();
  }

  return rpc_client.call(func_name, args...);
}

}

#endif