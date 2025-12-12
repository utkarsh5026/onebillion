package com.onebillion.strategies;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapUtils {
  static Map<String, StationResult> mergeMaps(List<Map<String, StationResult>> maps) {
    Map<String, StationResult> merged = new HashMap<>();

    for (Map<String, StationResult> map : maps) {
      for (Map.Entry<String, StationResult> entry : map.entrySet()) {
        merged
            .computeIfAbsent(entry.getKey(), k -> new StationResult(entry.getValue().stationName))
            .merge(entry.getValue());
      }
    }
    return merged;
  }
}
