package com.onebillion.strategies;

import org.jetbrains.annotations.NotNull;

record Station(String name, int value) {}

public class LineParser {
  static final char DELIMITER = ';';

  public static @NotNull Station parseLineStandard(@NotNull String line) {
    int commaIndex = line.indexOf(DELIMITER);
    String name = line.substring(0, commaIndex);
    int value = parseInt(line.substring(commaIndex + 1));
    return new Station(name, value);
  }

  private static int parseInt(String line) {
    int result = 0;
    for (int i = 0; i < line.length(); i++) {
      if (line.charAt(i) == DELIMITER) {
        continue;
      }
      char c = line.charAt(i);
      result = result * 10 + (c - '0');
    }
    return result;
  }

  static Station processLine(byte[] lineBuffer, int length) {
    int delimiterPos = -1;
    for (int i = 0; i < length; i++) {
      if (lineBuffer[i] == ';') {
        delimiterPos = i;
        break;
      }
    }

    if (delimiterPos == -1) throw new IllegalArgumentException("Invalid delimiter in line");

    var name = new String(lineBuffer, 0, delimiterPos);
    var value = parseValue(lineBuffer, delimiterPos + 1, length);

    return new Station(name, value);
  }

  private static int parseValue(byte[] lineBuffer, int startIndex, int length) {
    boolean negative = false;
    int index = startIndex;

    if (index < length && lineBuffer[index] == '-') {
      negative = true;
      index++;
    }

    int value = 0;
    while (index < length) {
      byte b = lineBuffer[index];
      if (b >= '0' && b <= '9') {
        value = value * 10 + (b - '0');
      }
      index++;
    }

    return negative ? -value : value;
  }
}
