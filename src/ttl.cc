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

    int byteToHexStr(unsigned char byte_arr[], int arr_len, unsigned char *HexStr, int *HexStrLen) {
      int i, index = 0;
      for (i = 0; i < arr_len; i++)
      {
        char hex1;
        char hex2;
        int value = byte_arr[i];
        int v1 = value / 16;
        int v2 = value % 16;
        if (v1 >= 0 && v1 <= 9)
          hex1 = (char)(48 + v1);
        else
          hex1 = (char)(55 + v1);
        if (v2 >= 0 && v2 <= 9)
          hex2 = (char)(48 + v2);
        else
          hex2 = (char)(55 + v2);
        if (*HexStrLen <= i)
        {
          return -1;
        }
        HexStr[index++] = hex1;
        HexStr[index++] = hex2;
      }
      *HexStrLen = index;
      return 0;
    }
  } // namespace titandb
} // namespace rocksdb