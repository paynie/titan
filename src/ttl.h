#pragma once
#include <stdint.h>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>

namespace rocksdb
{
  namespace titandb
  {
    void string_split(std::string str, const char split, std::vector<std::string>& res);
    std::string get_b2hex(const char * source, int len);
    // Transform bytes to long
    uint64_t BigBytesToLong(const char *bytes, int len);
    uint64_t ParseTTL(const char *bytes, int len);
    void longToBigBytes(uint64_t n, char *data, int len);
  } // namespace titandb
} // namespace rocksdb