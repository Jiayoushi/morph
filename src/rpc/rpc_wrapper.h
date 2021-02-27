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
    request_lost(0.0),
    reply_lost(0.0),
    generator(),
    distribution(0.0, 1.0),
    rpc_client(addr, port) {}

  void set_request_lost_probability(double prob) {
    request_lost = prob;
  }

  void set_reply_lost_probability(double prob) {
    reply_lost = prob;
  }

  template <typename... Args>
  RPCLIB_MSGPACK::object_handle call(std::string const &func_name,
                                       Args... args);


 private:
  /*
   * Fake network
   */
  double request_lost;
  double reply_lost;
  std::default_random_engine generator;
  std::uniform_real_distribution<double> distribution;

  int64_t default_timeout;

  rpc::client rpc_client;
};

template <typename... Args>
RPCLIB_MSGPACK::object_handle RpcClient::call(std::string const &func_name,
                                              Args... args) {
  RPCLIB_MSGPACK::object_handle object_handle;
  double random = distribution(generator);

  if (random < request_lost) {
    rpc_client.set_timeout(0);
  } else {
    rpc_client.clear_timeout();
  }

  object_handle = rpc_client.call(func_name, args...);

  // TODO: need a better way to throw timeout
  if (random < reply_lost) {
    rpc_client.set_timeout(0);
    rpc_client.call(func_name, args...);
  }

  return object_handle;
}

}

#endif