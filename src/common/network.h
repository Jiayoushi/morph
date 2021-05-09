#ifndef MORPH_COMMON_NETWORK_H
#define MORPH_COMMON_NETWORK_H

#include <string>

namespace morph {

using NetworkAddress = std::string;

bool verify_network_address(const NetworkAddress &addr);

} // namespace morph


#endif