package com.onebillion.strategies;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapUtils {
  static Map<Integer, StationResult> mergeMaps(List<Map<Integer, StationResult>> maps) {
    Map<Integer, StationResult> merged = new HashMap<>();

    for (Map<Integer, StationResult> map : maps) {
      for (Map.Entry<Integer, StationResult> entry : map.entrySet()) {
        merged
            .computeIfAbsent(entry.getKey(), k -> new StationResult(String.valueOf(entry.getKey())))
            .merge(entry.getValue());
      }
    }
    return merged;
  }
}
