package com.onebillion.strategies;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Zero-copy reader that directly parses data from memory-mapped segments. Uses SegmentProbing for
 * hash table operations and SWAR (SIMD Within A Register) for fast temperature parsing.
 */
public class ZeroCopyReader {

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

      // Create and process using SegmentProbing
      SegmentProbing probing = new SegmentProbing();
      probing.setSegment(segment);
      int rowCount = processSegment(segment, probing);

      // Extract results from the hash table
      Map<String, StationResult> results = probing.getResults();
      return new ZeroCopyStrategy.ChunkResult(results, rowCount);
    }
  }

  /**
   * Process a memory segment line by line, parsing station names and temperatures. Uses SWAR
   * optimization for fast temperature parsing.
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

      // Parse station name and compute hash
      byte b;
      while (start < fileSize && (b = segment.get(ValueLayout.JAVA_BYTE, start)) != ';') {
        hash ^= (b & 0xff);
        hash *= 0x01000193; // FNV-1a Prime
        start++;
      }

      int nameLen = (int) (start - nameStart);
      start++; // Skip semicolon

      // Parse temperature using SWAR (SIMD Within A Register) optimization
      int temp = parseTemperature(segment, fileSize, start);

      // Update start position based on temperature parsing
      start = getNextLineStart(segment, fileSize, start);

      // Store in hash table
      probing.put(segment, nameStart, nameLen, hash, temp);
      rowCount++;

      // Skip newline
      if (start < fileSize && segment.get(ValueLayout.JAVA_BYTE, start) == '\n') {
        start++;
      }
    }

    return rowCount;
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
    int temp;

    if (start + 8 <= fileSize) {
      // Fast path: Parse temperature using SWAR (SIMD Within A Register)
      long word = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, start);
      boolean neg = ((word & 0xFF) == '-');

      if (neg) {
        word >>>= 8;
      }

      if ((word >> 8 & 0xff) == '.') {
        // Format: X.X
        temp = (int) (((word & 0xff) - '0') * 10);
        temp += (int) ((word >> 16 & 0xff) - '0');
      } else {
        // Format: XX.X
        temp = (int) (((word & 0xff) - '0') * 100);
        temp += (int) (((word >> 8 & 0xff) - '0') * 10);
        temp += (int) ((word >> 24 & 0xff) - '0');
      }

      temp = neg ? -temp : temp;
    } else {
      // Slow path: parse byte by byte when near segment end
      temp = parseTemperatureFallback(segment, fileSize, start);
    }

    return temp;
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
  private long getNextLineStart(MemorySegment segment, long fileSize, long start) {
    // Skip remaining characters until newline
    while (start < fileSize) {
      byte ch = segment.get(ValueLayout.JAVA_BYTE, start);
      if (ch == '\n') {
        break;
      }
      start++;
    }
    return start;
  }
}
