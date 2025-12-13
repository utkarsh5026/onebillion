package com.onebillion.strategies;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FlatProbing implements LineReader {
  private final int mask;
  private final byte[][] keys; // Stores the station name bytes
  private final int[] hashes; // Cache the hash for quicker comparison
  private final long[] mins;
  private final long[] maxs;
  private final long[] sums;
  private final int[] counts;

  public FlatProbing(int tableSize) {
    this.mask = tableSize - 1;

    this.keys = new byte[tableSize][];
    this.hashes = new int[tableSize];
    this.mins = new long[tableSize];
    this.maxs = new long[tableSize];
    this.sums = new long[tableSize];
    this.counts = new int[tableSize];
  }

  private static boolean arrayEquals(byte[] data, int offset, byte[] storedKey, int length) {
    if (storedKey.length != length) return false;
    for (int i = 0; i < length; i++) {
      if (data[offset + i] != storedKey[i]) return false;
    }
    return true;
  }

  @Override
  public void readLine(byte[] lineBytes, int start, int end) {
    int semicolonPos = getSemiColonIndex(lineBytes, start, end);
    if (semicolonPos == -1) {
      throw new IllegalArgumentException("Invalid input: no semicolon found");
    }

    int nameLen = semicolonPos - start;
    int hash = HashUtils.hashFnvDirect(lineBytes, start, nameLen);
    long temp = getTemp(semicolonPos, lineBytes, end);
    probe(lineBytes, start, nameLen, hash, temp);
  }

  private void probe(byte[] data, int nameOffset, int nameLen, int hash, long temp) {
    int index = hash & mask;

    while (true) {
      if (keys[index] == null) {
        insertNew(data, nameOffset, nameLen, hash, temp, index);
        return;
      }

      if (hashes[index] == hash && arrayEquals(data, nameOffset, keys[index], nameLen)) {
        updateExisting(index, temp);
        return;
      }

      index = (index + 1) & mask;
    }
  }

  private void updateExisting(int index, long temp) {
    if (temp < mins[index]) mins[index] = temp;
    if (temp > maxs[index]) maxs[index] = temp;
    sums[index] += temp;
    counts[index]++;
  }

  private void insertNew(byte[] data, int nameOffset, int nameLen, int hash, long temp, int index) {
    byte[] nameBytes = new byte[nameLen];
    System.arraycopy(data, nameOffset, nameBytes, 0, nameLen);
    keys[index] = nameBytes;
    hashes[index] = hash;
    mins[index] = temp;
    maxs[index] = temp;
    sums[index] = temp;
    counts[index] = 1;
  }

  @Override
  public Map<String, StationResult> collect() {
    return IntStream.range(0, keys.length)
        .filter(i -> keys[i] != null)
        .boxed()
        .collect(
            Collectors.toMap(
                i -> new String(keys[i], StandardCharsets.UTF_8),
                i -> {
                  var sr = new StationResult(new String(keys[i], StandardCharsets.UTF_8));
                  sr.min = mins[i];
                  sr.max = maxs[i];
                  sr.sum = sums[i];
                  sr.count = counts[i];
                  return sr;
                }));
  }
}
