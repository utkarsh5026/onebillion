package com.onebillion.strategies;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.HashMap;
import java.util.Map;

public class SegmentProbing {
  private static final int TABLE_SIZE = 1 << 16;
  private static final int TABLE_MASK = TABLE_SIZE - 1;

  private final Entry[] table = new Entry[TABLE_SIZE];
  private MemorySegment segment; // Store reference to the memory segment

  public void setSegment(MemorySegment segment) {
    this.segment = segment;
  }

  public void put(MemorySegment segment, long nameOffset, int len, int hash, int val) {
    int idx = hash & TABLE_MASK;
    while (true) {
      Entry e = table[idx];
      if (e == null) {
        table[idx] = new Entry(nameOffset, len, val);
        return;
      }
      if (e.nameLen == len && isEqual(segment, nameOffset, e)) {
        e.update(val);
        return;
      }
      idx = (idx + 1) & TABLE_MASK;
    }
  }

  private boolean isEqual(MemorySegment segment, long offset, Entry e) {
    return MemorySegment.mismatch(segment, offset, offset + e.nameLen,
                                  segment, e.nameOffset, e.nameOffset + e.nameLen) == -1;
  }

  public Map<String, StationResult> getResults() {
    Map<String, StationResult> results = new HashMap<>();
    for (Entry e : table) {
      if (e != null) {
        // Extract the station name from the memory segment
        byte[] nameBytes = new byte[e.nameLen];
        for (int i = 0; i < e.nameLen; i++) {
          nameBytes[i] = segment.get(ValueLayout.JAVA_BYTE, e.nameOffset + i);
        }
        String stationName = new String(nameBytes);

        // Create StationResult and set values directly
        StationResult result = new StationResult(stationName);
        result.min = e.min;
        result.max = e.max;
        result.sum = e.sum;
        result.count = e.count;

        results.put(stationName, result);
      }
    }
    return results;
  }

  static class Entry {
    long nameOffset;
    int nameLen;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long sum = 0;
    long count = 0;

    Entry(long nameOffset, int nameLen, int val) {
      this.nameOffset = nameOffset;
      this.nameLen = nameLen;
      this.sum = val;
      this.count = 1;
      this.min = val;
      this.max = val;
    }

    void update(int val) {
      if (val < min)
        min = val;
      if (val > max)
        max = val;
      sum += val;
      count++;
    }
  }
}
