package com.onebillion.strategies;

import java.util.Map;

public interface LineReader {
  void readLine(byte[] lineBytes, int end);

  Map<String, StationResult> collect();

  default long getTemp(int semicolonPos, int length, byte[] nameByte, int nameLen) {
    long temp = 0;
    boolean negative = false;
    for (int i = semicolonPos + 1; i < nameLen; i++) {
      byte b = nameByte[i];
      if (b == '-') {
        negative = true;
      } else if (b != '.') {
        temp = temp * 10 + (b - '0');
      }
    }

    if (negative) {
      temp = -temp;
    }

    return temp;
  }

  default int getSemiColonIndex(byte[] lineBytes, int end) {
    for (int i = 0; i < end; i++) {
      if (lineBytes[i] == ';') {
        return i;
      }
    }
    return -1;
  }
}
