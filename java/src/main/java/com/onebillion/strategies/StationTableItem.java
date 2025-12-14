package com.onebillion.strategies;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class StationTableItem {
  byte[] name;
  int hash;
  long sum;
  long count;
  long maximum;
  long minimum;

  StationTableItem(byte[] name, long val, int hash) {
    this.name = name;
    this.sum = val;
    this.count = 1;
    this.maximum = val;
    this.minimum = val;
    this.hash = hash;
  }

  public static Map<String, StationResult> toResultMap(StationTableItem[] table, int occupied) {
    return Arrays.stream(table)
        .filter(Objects::nonNull)
        .collect(
            Collectors.toMap(
                item -> new String(item.name, StandardCharsets.UTF_8),
                item -> {
                  var result = new StationResult(new String(item.name, StandardCharsets.UTF_8));
                  result.sum = item.sum;
                  result.count = item.count;
                  result.max = item.maximum;
                  result.min = item.minimum;
                  return result;
                },
                (a, _) -> a,
                () -> new HashMap<>(occupied)));
  }

  void update(long temp) {
    this.sum += temp;
    this.count++;
    if (temp > this.maximum) this.maximum = temp;
    if (temp < this.minimum) this.minimum = temp;
  }
}
