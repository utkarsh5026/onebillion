package com.onebillion.strategies;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;
import org.jetbrains.annotations.NotNull;

public class LinearProbing {
  private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_128;

  private final int mask;
  private final StationTableItem[] table;
  private int occupied;

  public LinearProbing(int tableSize) {
    this.mask = tableSize - 1;
    this.table = new StationTableItem[tableSize];
    this.occupied = 0;
  }

  /**
   * SIMD-optimized byte array comparison using Vector API. Compares 16 bytes at a time instead of
   * byte-by-byte.
   */
  private static boolean vectorEquals(byte @NotNull [] a, byte @NotNull [] b) {
    if (a.length != b.length) return false;

    int len = a.length;
    int i = 0;

    int upperBound = SPECIES.loopBound(len);
    for (; i < upperBound; i += SPECIES.length()) {
      var va = ByteVector.fromArray(SPECIES, a, i);
      var vb = ByteVector.fromArray(SPECIES, b, i);
      if (!va.eq(vb).allTrue()) {
        return false;
      }
    }

    for (; i < len; i++) {
      if (a[i] != b[i]) return false;
    }

    return true;
  }

  public void probe(byte[] name, int hash, long temp) {
    int index = hash & mask;
    while (true) {
      var item = table[index];
      if (item == null) {
        table[index] = new StationTableItem(name, temp, hash);
        this.occupied++;
        return;
      }
      if (item.hash == hash && vectorEquals(item.name, name)) {
        item.update(temp);
        return;
      }
      index = (index + 1) & mask;
    }
  }

  public Map<String, StationResult> toMap() {
    return Arrays.stream(table)
        .filter(Objects::nonNull)
        .collect(
            Collectors.toMap(
                item -> new String(item.name),
                item -> {
                  var result = new StationResult(new String(item.name));
                  result.sum = item.sum;
                  result.count = item.count;
                  result.max = item.maximum;
                  result.min = item.minimum;
                  return result;
                },
                (a, b) -> a,
                () -> new HashMap<>(occupied)));
  }

  static class StationTableItem {
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

    void update(long temp) {
      this.sum += temp;
      this.count++;
      if (temp > this.maximum) this.maximum = temp;
      if (temp < this.minimum) this.minimum = temp;
    }
  }
}
