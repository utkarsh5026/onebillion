package com.onebillion.strategies;

class HashUtils {
  public static int hashFnvDirect(byte[] nameData, int offset, int length) {
    int hash = 0x811c9dc5;
    for (int i = offset; i < offset + length; i++) {
      byte b = nameData[i];
      hash ^= (b & 0xff);
      hash *= 0x01000193;
    }
    return hash;
  }
}
