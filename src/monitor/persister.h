#ifndef MORPH_PAXOS_PERSISTER_H
#define MORPH_PAXOS_PERSISTER_H

#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <cassert>
#include <sys/types.h>
#include <sys/stat.h>

namespace morph {
namespace paxos {

class Persister {
 public:
  Persister(const std::string &filename):
      fd(-1),
      persisted() {
    fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_SYNC, 0666);
    if (fd < 0) {
      perror("open");
      assert(false);
    }

    read_from_disk();
  }

  ~Persister() {
    assert(close(fd) == 0);
  }

  void persist(std::string &&value) {
    persisted = std::move(value);
    write_to_disk();
  }

  bool empty() const {
    return persisted.empty();
  }

  size_t size() const {
    return persisted.size();
  }

  void get_persisted(std::string *value) {
    *value = persisted;
  }

 private:
  void write_to_disk();

  void read_from_disk();

  int fd;
  std::string persisted;
};

} // namespace paxos
} // namespace morph

#endif