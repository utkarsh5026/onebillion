package com.onebillion.strategies;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class HashTable implements LineReader {
  private static final int INITIAL_CAPACITY = 400; // number of expected unique stations
  private final Map<String, StationResult> table;

  public HashTable() {
    this.table = new HashMap<>(INITIAL_CAPACITY);
  }

  @Override
  public void readLine(byte[] lineBytes, int start, int end) {
    for (int i = start; i < end; i++) {
      if (lineBytes[i] == ';') {
        int nameLen = i - start;
        var stationName = new String(lineBytes, start, nameLen, StandardCharsets.UTF_8);
        long measurement = getTemp(i, lineBytes, end);
        StationResult result =
            table.computeIfAbsent(stationName, _ -> new StationResult(stationName));
        result.add(measurement);
        break;
      }
    }
  }

  @Override
  public Map<String, StationResult> collect() {
    return table;
  }
}
