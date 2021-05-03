#ifndef MORPH_FILENAME_H
#define MORPH_FILENAME_H

#include <cstdint>
#include <string>

namespace morph {

std::string log_file_name(const std::string &name, uint64_t number);

uint64_t get_number(const std::string &filename);

} // namespace morph

#endif