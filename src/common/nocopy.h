#ifndef MORPH_NOCOPY_H
#define MORPH_NOCOPY_H

namespace morph {
  
class NoCopy {
 public:
  NoCopy(const NoCopy &) = delete;
  void operator=(const NoCopy &) = delete;

 protected:
  NoCopy() = default;
  ~NoCopy() = default;
};


}


#endif