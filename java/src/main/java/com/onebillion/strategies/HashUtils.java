package com.onebillion.strategies;

class HashUtils {
  public static int hashFnvDirect(byte[] nameData, int start, int len) {
    int hash = 0x811c9dc5;
    for (int i = start; i < start + len; i++) {
      byte b = nameData[i];
      hash ^= (b & 0xff);
      hash *= 0x01000193;
    }
    return hash;
  }

  public static int simpleHash(byte[] data, int start, int len) {
    int h = len;
    if (len >= 4) {
      int first4 =
          (data[start] & 0xFF)
              | ((data[start + 1] & 0xFF) << 8)
              | ((data[start + 2] & 0xFF) << 16)
              | ((data[start + 3] & 0xFF) << 24);
      h ^= first4;
    } else {
      for (int i = 0; i < len; i++) h = 31 * h + data[start + i];
    }
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    return h;
  }
}
