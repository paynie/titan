#pragma once
#include <stdint.h>

namespace rocksdb
{
  namespace titandb
  {
    std::string get_b2hex(const char * source, int len);
    // Transform bytes to long
    uint64_t BigBytesToLong(const char *bytes, int len);
    uint64_t ParseTTL(const char *bytes, int len);
    void longToBigBytes(uint64_t n, char *data, int len);
  } // namespace titandb
} // namespace rocksdb