#ifndef MORPH_FILENAME_H
#define MORPH_FILENAME_H

#include <cstdint>
#include <string>

namespace morph {

std::string block_store_file_name(const std::string &name); 

std::string kv_db_file_name(const std::string &name);

std::string kv_wal_file_name(const std::string &name);

std::string logger_file_name(const std::string &name);

std::string log_file_name(const std::string &name, uint64_t number);

uint64_t get_filename_number(const std::string &filename);

std::string get_filename_suffix(const std::string &filename);

} // namespace morph

#endif