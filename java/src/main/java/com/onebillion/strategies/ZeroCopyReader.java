package com.onebillion.strategies;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Zero-copy reader that directly parses data from memory-mapped segments. Uses SegmentProbing for
 * hash table operations and SWAR (SIMD Within A Register) for fast temperature parsing.
 */
public class ZeroCopyReader {

  private static final long NEW_LINE_WORD_MASK = 0x0A0A0A0A0A0A0A0AL; // 0x0A is '\n'
  private static final long SEMI_COLON_WORD_MASK = 0x3B3B3B3B3B3B3B3BL; // 0x3B is ';'

  /**
   * Process a chunk of the file using zero-copy memory mapping.
   *
   * @param chunk the chunk to process containing filepath, start, and end offsets
   * @return ChunkResult containing the parsed results and row count
   * @throws IOException if an I/O error occurs
   */
  public ZeroCopyStrategy.ChunkResult processChunk(ZeroCopyStrategy.Chunk chunk)
      throws IOException {
    Path path = Paths.get(chunk.filepath());
    try (var arena = Arena.ofConfined();
        var channel = FileChannel.open(path)) {
      long size = chunk.end() - chunk.start();
      var segment = channel.map(FileChannel.MapMode.READ_ONLY, chunk.start(), size, arena);

      var probing = new SegmentProbing();
      probing.setSegment(segment);
      int rowCount = processSegment(segment, probing);

      var results = probing.getResults();
      return new ZeroCopyStrategy.ChunkResult(results, rowCount);
    }
  }

  /**
   * Process a memory segment line by line, parsing station names and temperatures. Uses SWAR
   * optimization for fast semicolon finding and temperature parsing.
   *
   * @param segment the memory segment to process
   * @param probing the hash table to store results
   * @return the number of rows processed
   */
  private int processSegment(MemorySegment segment, SegmentProbing probing) {
    long fileSize = segment.byteSize();
    long start = 0;
    int rowCount = 0;

    while (start < fileSize) {
      long nameStart = start;
      int hash = 0x811c9dc5; // FNV-1a Init

      long result = findSemicolonSWAR(segment, fileSize, start, hash);
      start = result & 0xFFFFFFFFL; // Extract position (lower 32 bits)
      hash = (int) (result >>> 32); // Extract hash (upper 32 bits)

      int nameLen = (int) (start - nameStart);
      start++;

      int temp = parseTemperature(segment, fileSize, start);
      start = getNextLineStartSWAR(segment, fileSize, start);

      probing.put(segment, nameStart, nameLen, hash, temp);
      rowCount++;

      if (start < fileSize && segment.get(ValueLayout.JAVA_BYTE, start) == '\n') {
        start++;
      }
    }

    return rowCount;
  }

  /**
   * Find semicolon using SWAR (SIMD Within A Register) optimization. Scans 8 bytes at a time and
   * updates the FNV-1a hash for bytes before the semicolon.
   *
   * <p>Returns a packed long: upper 32 bits = hash, lower 32 bits = position. This avoids object
   * allocation for better performance.
   *
   * @param segment the memory segment
   * @param fileSize the size of the segment
   * @param start the starting position
   * @param hash the current FNV-1a hash value
   * @return packed long with hash in upper 32 bits and position in lower 32 bits
   */
  private long findSemicolonSWAR(MemorySegment segment, long fileSize, long start, int hash) {
    long pos = start;

    while (pos + 8 <= fileSize) {
      long word = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, pos);

      long xor = word ^ SEMI_COLON_WORD_MASK;
      long hasSemicolon = (xor - 0x0101010101010101L) & ~xor & 0x8080808080808080L;

      if (hasSemicolon != 0) {
        int semicolonOffset = Long.numberOfTrailingZeros(hasSemicolon) >>> 3;

        for (int i = 0; i < semicolonOffset; i++) {
          int b = (int) (word >> (i * 8)) & 0xff;
          hash ^= b;
          hash *= 0x01000193; // FNV-1a Prime
        }

        return ((long) hash << 32) | (pos + semicolonOffset);
      }

      for (int i = 0; i < 8; i++) {
        int b = (int) (word >> (i * 8)) & 0xff;
        hash ^= b;
        hash *= 0x01000193; // FNV-1a Prime
      }

      pos += 8;
    }

    while (pos < fileSize) {
      byte b = segment.get(ValueLayout.JAVA_BYTE, pos);
      if (b == ';') {
        return ((long) hash << 32) | pos;
      }
      hash ^= (b & 0xff);
      hash *= 0x01000193; // FNV-1a Prime
      pos++;
    }

    return ((long) hash << 32) | pos;
  }

  /**
   * Parse temperature using SWAR (SIMD Within A Register) optimization. Reads 8 bytes at once when
   * possible for faster parsing.
   *
   * @param segment the memory segment
   * @param fileSize the size of the segment
   * @param start the starting position
   * @return the parsed temperature value (multiplied by 10 to preserve one decimal place)
   */
  private int parseTemperature(MemorySegment segment, long fileSize, long start) {
    if (start + 8 > fileSize) return parseTemperatureFallback(segment, fileSize, start);

    int temp;

    long word = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, start);
    boolean neg = ((word & 0xFF) == '-');

    if (neg) {
      word >>>= 8;
    }

    if ((word >> 8 & 0xff) == '.') {
      temp = (int) (((word & 0xff) - '0') * 10);
      temp += (int) ((word >> 16 & 0xff) - '0');
    } else {
      temp = (int) (((word & 0xff) - '0') * 100);
      temp += (int) (((word >> 8 & 0xff) - '0') * 10);
      temp += (int) ((word >> 24 & 0xff) - '0');
    }

    return neg ? -temp : temp;
  }

  /**
   * Fallback temperature parser for cases near segment end.
   *
   * @param segment the memory segment
   * @param fileSize the size of the segment
   * @param start the starting position
   * @return the parsed temperature value
   */
  private int parseTemperatureFallback(MemorySegment segment, long fileSize, long start) {
    boolean neg = false;
    if (start < fileSize && segment.get(ValueLayout.JAVA_BYTE, start) == '-') {
      neg = true;
      start++;
    }

    int temp = 0;
    while (start < fileSize) {
      byte ch = segment.get(ValueLayout.JAVA_BYTE, start);
      if (ch == '.') {
        start++;
        if (start < fileSize) {
          temp = temp * 10 + (segment.get(ValueLayout.JAVA_BYTE, start) - '0');
        }
        break;
      }
      temp = temp * 10 + (ch - '0');
      start++;
    }

    return neg ? -temp : temp;
  }

  /**
   * Find the start of the next line after parsing temperature.
   *
   * @param segment the memory segment
   * @param fileSize the size of the segment
   * @param start the current position
   * @return the position of the next line start
   */
  private long getNextLineStartSWAR(MemorySegment segment, long fileSize, long start) {
    long limit = fileSize - 8;

    while (start < limit) {
      long word = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, start);
      long input =
          word
              ^ NEW_LINE_WORD_MASK; // XOR with the newline mask. Any byte that was '\n' (0x0A)
                                    // becomes
      // 0x00.

      // "Has Zero Byte" bitwise trick:
      // (x - 0x01...) & ~x & 0x80... detects if any byte is 0x00
      long tmp = (input - 0x0101010101010101L) & ~input & 0x8080808080808080L;

      if (tmp != 0) {
        if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
          return start + (Long.numberOfLeadingZeros(tmp) >>> 3);
        } else {
          return start + (Long.numberOfTrailingZeros(tmp) >>> 3);
        }
      }
      start += 8;
    }

    while (start < fileSize) {
      if (segment.get(ValueLayout.JAVA_BYTE, start) == '\n') {
        break;
      }
      start++;
    }
    return start;
  }
}