#include "network.h"

#include <string>

namespace morph {

bool verify_network_address(const NetworkAddress &addr) {
  uint16_t numbers[5];   // [0].[1].[2].[3]:[4]
  int semicolon = 0, dot = 0;
  int number_count = 0;
  int num = 0;
  
  for (int i = 0; i < addr.size(); ++i) {
    if (isdigit(addr[i])) {
      num = num * 10 + (addr[i] - '0');
    } else if (addr[i] == '.') {
      numbers[number_count++] = num;
      ++dot;
      num = 0;
    } else if (addr[i] == ':') {
      numbers[number_count++] = num;
      ++semicolon;
      num = 0;
      // If there is no character after :
      if (i + 1 == addr.size()) {
        return false;
      }
    } else {
      return false;
    }
  }
  numbers[number_count++] = num;

  return number_count == 5 && dot == 3 && semicolon == 1 
    && numbers[0] <= 255 && numbers[1] <= 255 && numbers[2] <= 255 
    && numbers[3] <= 255 && numbers[4] <= 65535;
}

} // namespace morph