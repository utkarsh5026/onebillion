package com.onebillion.strategies;

import java.util.Arrays;
import java.util.Map;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

public class SIMDHashTable implements LineReader {
  private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
  private static final int TABLE_SIZE = 131072; // 2^17
  private final int mask;
  private final StationTableItem[] table;
  private int occupied;

  public SIMDHashTable() {
    this.mask = TABLE_SIZE - 1;
    this.table = new StationTableItem[TABLE_SIZE];
    this.occupied = 0;
  }

  /** SIMD BYTE ARRAY COMPARISON - Much faster than scalar loop */
  private static boolean arrayEqualsSIMD(byte[] data, byte[] key, int start, int end) {
    if (end - start != key.length) {
      return false;
    }

    return Arrays.mismatch(data, start, end, key, 0, key.length) == -1;
  }

  @Override
  public void readLine(byte[] lineBytes, int start, int end) {
    int semicolonPos = findSemicolon(lineBytes, start, end);
    if (semicolonPos == -1) {
      throw new IllegalArgumentException("Invalid input: no semicolon found");
    }

    int hash = HashUtils.simpleHash(lineBytes, start, semicolonPos - start);
    long temp = parseTempSWAR(lineBytes, semicolonPos + 1);
    int nameLen = semicolonPos - start;
    probeSIMD(lineBytes, start, nameLen, hash, temp);
  }

  private int findSemicolon(byte[] data, int start, int end) {
    int vectorSize = BYTE_SPECIES.length();
    byte semicolon = (byte) ';';
    int i = start;

    while (i <= end - vectorSize) {
      var vector = ByteVector.fromArray(BYTE_SPECIES, data, i);
      VectorMask<Byte> mask = vector.eq(semicolon);

      if (mask.anyTrue()) {
        return i + mask.firstTrue();
      }

      i += vectorSize;
    }

    while (i < end) {
      if (data[i] == semicolon) {
        return i;
      }
      i++;
    }

    return -1;
  }

  /** SIMD-OPTIMIZED PROBING with vectorized byte comparison */
  private void probeSIMD(byte[] data, int nameOffset, int nameLen, int hash, long temp) {
    int index = hash & mask;
    while (true) {
      StationTableItem item = table[index];
      if (item == null) {
        createNewItem(data, nameOffset, nameLen, hash, temp, index);
        return;
      }

      if (item.hash == hash && arrayEqualsSIMD(data, item.name, nameOffset, nameOffset + nameLen)) {
        item.update(temp);
        return;
      }
      index = (index + 1) & mask;
    }
  }

  /** SIMD TEMPERATURE PARSING - Parse digits in parallel Temperature format: -?[0-9]+\.[0-9] */
  private long parseTempSWAR(byte[] data, int start) {
    int byteLength = 8;
    long word = 0;

    int i = start;
    int limit = Math.min(data.length, byteLength + start);
    while (i < limit) {
      word |= ((long) (data[i] & 0xFF)) << ((i - start) * byteLength);
      i++;
    }

    long signMask = word & 0xFF;
    boolean negative = signMask == (long) '-';
    if (negative) {
      word >>>= 8;
    }

    long val;
    byte b1 = (byte) (word & 0xFF);
    val = b1 - '0';

    byte b2 = (byte) ((word >>> 8) & 0xFF);
    if (b2 == '.') {
      // Format: X.X
      byte b3 = (byte) ((word >>> 16) & 0xFF);
      val = val * 10 + (b3 - '0');
    } else {
      // Format: XX.X
      val = val * 10 + (b2 - '0');
      // b3 is dot, skip
      byte b4 = (byte) ((word >>> 24) & 0xFF);
      val = val * 10 + (b4 - '0');
    }

    return negative ? -val : val;
  }

  private void createNewItem(
      byte[] data, int nameOffset, int nameLen, int hash, long temp, int index) {
    byte[] name = new byte[nameLen];
    System.arraycopy(data, nameOffset, name, 0, nameLen);
    table[index] = new StationTableItem(name, temp, hash);
    this.occupied++;
  }

  @Override
  public Map<String, StationResult> collect() {
    return StationTableItem.toResultMap(this.table, this.occupied);
  }
}
