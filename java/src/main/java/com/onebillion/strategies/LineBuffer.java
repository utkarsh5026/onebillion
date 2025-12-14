package com.onebillion.strategies;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class LineBuffer {
  private static final int DEFAULT_SIZE = 128;
  private static final int SPILLOVER_SIZE = 16384; // 16KB to handle batch sizes
  // PREFERRED species will choose the widest SIMD available (e.g., AVX2 or
  // AVX-512)
  private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;

  // Global SIMD setting - can be configured once at program startup
  private static boolean USE_SIMD_DEFAULT = false;

  final byte[] spillOver;
  final LineReader reader;
  private final byte[] buffer;
  private final boolean useSIMD;
  int spillOverPosition = 0;
  int filled = 0;
  private int position = 0;

  public LineBuffer(int size, LineReader reader, boolean useSIMD) {
    this.buffer = new byte[size];
    this.spillOver = new byte[SPILLOVER_SIZE];
    this.reader = reader;
    this.useSIMD = useSIMD;
  }

  public LineBuffer(LineReader reader) {
    this(DEFAULT_SIZE, reader, USE_SIMD_DEFAULT);
  }

  /**
   * Set the default SIMD mode for all LineBuffer instances. Call this once at program startup.
   *
   * @param enabled true to enable SIMD by default, false otherwise
   */
  public static void setUseSIMD(boolean enabled) {
    USE_SIMD_DEFAULT = enabled;
  }

  public void processBuffer(byte[] buf, int length) {
    if (useSIMD) {
      processBufferSIMD(buf, length);
    } else {
      processBufferScalar(buf, length);
    }
  }

  private void processBufferScalar(byte[] buf, int length) {
    for (int i = 0; i < length; i++) {
      byte b = buf[i];
      if (b == '\n' || b == '\r') {
        if (reader != null && position > 0) {
          reader.readLine(buffer, 0, position);
          filled++;
        }
        position = 0;
        continue;
      }

      if (position < buffer.length) {
        buffer[position++] = b;
      }
    }
  }

  private void processScalar(byte[] buf, int length) {
    int start = 0;

    for (int i = 0; i < length; i++) {
      byte b = buf[i];
      if (b == '\n') {
        if (spillOverPosition > 0) {
          // We have incomplete line from previous batch - combine and process
          int remainingLen = i - start;
          drainSpillOver(buf, start, i, remainingLen);
          spillOverPosition = 0;
        } else {
          reader.readLine(buf, start, i);
        }
        filled++;
        start = i + 1;
      }
    }

    // Handle any remaining data in the buffer
    if (start < length) {
      fillSpillOver(buf, start, length);
    }
  }

  private void processBufferSIMD(byte[] buf, int length) {
    int start = 0;

    while (start < length) {
      int newLinePos = findNewLine(buf, start, length);
      if (newLinePos == -1) {
        // No newline found in remaining buffer - save to spillover
        fillSpillOver(buf, start, length);
        break;
      }

      if (spillOverPosition > 0) {
        // We have incomplete line from previous batch - combine and process
        int remainingLen = newLinePos - start;
        drainSpillOver(buf, start, newLinePos, remainingLen);
      } else {
        reader.readLine(buf, start, newLinePos);
      }
      filled++;
      start = newLinePos + 1;
    }
  }

  /** Flush any remaining data in spillover. Call this after processing all buffers. */
  public void flush() {
    if (spillOverPosition > 0 && reader != null) {
      reader.readLine(spillOver, 0, spillOverPosition);
      filled++;
      spillOverPosition = 0;
    }
  }

  private int findNewLine(byte[] buffer, int start, int limit) {
    int i = start;
    int loopBound = limit - SPECIES.length();

    for (; i <= loopBound; i += SPECIES.length()) {
      var v = ByteVector.fromArray(SPECIES, buffer, i);
      long mask = v.compare(VectorOperators.EQ, (byte) '\n').toLong();
      if (mask != 0) {
        return i + Long.numberOfTrailingZeros(mask);
      }
    }

    for (; i < limit; i++) {
      if (buffer[i] == '\n') {
        return i;
      }
    }

    return -1;
  }

  public int getFilled() {
    return filled;
  }

  private void fillSpillOver(byte[] buffer, int start, int end) {
    int bytesToCopy = Math.min(end - start, spillOver.length - spillOverPosition);
    if (bytesToCopy > 0) {
      System.arraycopy(buffer, start, spillOver, spillOverPosition, bytesToCopy);
      spillOverPosition += bytesToCopy;
    }
    // Silently truncate if line exceeds buffer (matches scalar behavior)
  }

  private void drainSpillOver(byte[] buf, int start, int end, int remainingLen) {
    var lineByte = new byte[spillOverPosition + remainingLen];
    System.arraycopy(spillOver, 0, lineByte, 0, spillOverPosition);
    System.arraycopy(buf, start, lineByte, spillOverPosition, remainingLen);
    reader.readLine(lineByte, 0, lineByte.length);
    spillOverPosition = 0;
  }
}
