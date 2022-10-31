#include "ttl.h"

namespace rocksdb
{
  namespace titandb
  {
    // Transform bytes to long
    uint64_t BigBytesToLong(const char *bytes, int len)
    {
      if(len < 8) {
        return 0;
      }

      uint64_t n = 0;
      n += (uint64_t)(bytes[0] & 255) << 56;
      n += (uint64_t)(bytes[1] & 255) << 48;
      n += (uint64_t)(bytes[2] & 255) << 40;
      n += (uint64_t)(bytes[3] & 255) << 32;
      n += (uint64_t)(bytes[4] & 255) << 24;
      n += (uint64_t)(bytes[5] & 255) << 16;
      n += (uint64_t)(bytes[6] & 255) << 8;
      n += (uint64_t)(bytes[7] & 255);
      return n;
    }

    uint64_t ParseTTL(const char *bytes, int len)
    {
      if (len < 8)
      {
        return 0;
      }

      return BigBytesToLong(bytes + (len - 8), 8);
    }

    void longToBigBytes(uint64_t n, char *data, int len)
    {
      if(len < 8) {
        return;
      }
      
      data[0] = (char)(n >> 56 & 0xff);
      data[1] = (char)(n >> 48 & 0xff);
      data[2] = (char)(n >> 40 & 0xff);
      data[3] = (char)(n >> 32 & 0xff);
      data[4] = (char)(n >> 24 & 0xff);
      data[5] = (char)(n >> 16 & 0xff);
      data[6] = (char)(n >> 8 & 0xff);
      data[7] = (char)(n & 0xff);
    }
  } // namespace titandb
} // namespace rocksdb