package com.onebillion.strategies;

import java.util.HashMap;
import java.util.Map;

public class HashTable implements LineReader {
  private static final int INITIAL_CAPACITY = 400; // number of expected unique stations
  private final Map<String, StationResult> table;

  public HashTable() {
    this.table = new HashMap<>(INITIAL_CAPACITY);
  }

  @Override
  public void readLine(byte[] lineBytes, int end) {
    for (int i = 0; i < end; i++) {
      if (lineBytes[i] == ';') {
        var stationName = new String(lineBytes, 0, i);
        long measurement = getTemp(i, end, lineBytes, end);
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
