package com.onebillion.strategies;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MCMPOptimizedStrategy implements Strategy {
  private static final int TABLE_SIZE = 131072; // 2^17, same as Go
  private static final int TABLE_MASK = TABLE_SIZE - 1;
  private static final int BUFFER_SIZE = 1024 * 1024; // 1MB buffers like Go

  private final int processors = Runtime.getRuntime().availableProcessors();

  @Override
  public List<StationResult> Analyze(String filepath) throws IOException {
    var fileSize = Files.size(Paths.get(filepath));
    var chunkSize = fileSize / processors;

    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = launchWorkers(filepath, chunkSize, fileSize, executor);
      var results = waitForResult(futures);
      return MapUtils.mergeMaps(results).values().stream().toList();
    }
  }

  private List<Future<Map<String, StationResult>>> launchWorkers(
      String filepath, long chunkSize, long fileSize, ExecutorService executor) {
    List<Future<Map<String, StationResult>>> futures = new ArrayList<>(processors);
    for (int i = 0; i < processors; i++) {
      long start = (long) i * chunkSize;
      long end = (i == processors - 1) ? fileSize : start + chunkSize;

      final boolean isFirst = i == 0;
      final boolean isLast = i == processors - 1;

      futures.add(executor.submit(() -> processChunk(filepath, start, end, isFirst, isLast)));
    }
    return futures;
  }

  private List<Map<String, StationResult>> waitForResult(
      List<Future<Map<String, StationResult>>> futures) {
    List<Map<String, StationResult>> results = new ArrayList<>();
    for (var future : futures) {
      try {
        results.add(future.get());
      } catch (Exception e) {
        throw new RuntimeException("Error in thread execution: " + e.getMessage(), e);
      }
    }
    return results;
  }

  private Map<String, StationResult> processChunk(
      String filepath, long start, long end, boolean isFirst, boolean isLast) throws IOException {

    // Linear probing hash table - like Go
    StationTableItem[] items = new StationTableItem[TABLE_SIZE];
    for (int i = 0; i < TABLE_SIZE; i++) {
      items[i] = new StationTableItem();
    }
    List<Integer> occupiedIndexes = new ArrayList<>(10000);

    try (RandomAccessFile raf = new RandomAccessFile(filepath, "r");
        FileChannel channel = raf.getChannel()) {

      // Adjust start position to line boundary
      if (!isFirst) {
        raf.seek(start - 1);
        byte[] temp = new byte[1];
        raf.read(temp);
        if (temp[0] != '\n') {
          // Find next newline
          while (start < end) {
            raf.read(temp);
            start++;
            if (temp[0] == '\n') {
              break;
            }
          }
        }
      }

      ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
      byte[] leftover = new byte[0];

      long currentPos = start;
      while (currentPos < end) {
        buffer.clear();
        int bytesRead = channel.read(buffer, currentPos);
        if (bytesRead <= 0) break;

        buffer.flip();
        byte[] chunk = new byte[buffer.remaining()];
        buffer.get(chunk);

        // Combine leftover with new chunk
        byte[] data;
        if (leftover.length > 0) {
          data = new byte[leftover.length + chunk.length];
          System.arraycopy(leftover, 0, data, 0, leftover.length);
          System.arraycopy(chunk, 0, data, leftover.length, chunk.length);
        } else {
          data = chunk;
        }

        int pos = 0;
        while (pos < data.length) {
          // Find line end
          int lineEnd = pos;
          while (lineEnd < data.length && data[lineEnd] != '\n') {
            lineEnd++;
          }

          // If we didn't find a newline, save as leftover (unless we're past chunk end for non-last
          // chunks)
          if (lineEnd >= data.length) {
            if (!isLast) {
              // Don't carry incomplete lines past the end boundary
              break;
            }
            leftover = Arrays.copyOfRange(data, pos, data.length);
            break;
          }

          // Process line
          byte[] line = Arrays.copyOfRange(data, pos, lineEnd);
          processLine(line, items, occupiedIndexes);

          pos = lineEnd + 1;
        }

        currentPos += bytesRead;

        // Stop reading if we've passed the chunk end (non-last chunks)
        if (!isLast && currentPos >= end) {
          break;
        }
      }

      // Process final leftover for last chunk only
      if (isLast && leftover.length > 0) {
        processLine(leftover, items, occupiedIndexes);
      }

      // Convert hash table to map
      Map<String, StationResult> resultMap = new HashMap<>(occupiedIndexes.size());
      for (int idx : occupiedIndexes) {
        StationTableItem item = items[idx];
        String stationName = new String(item.name);
        StationResult result = new StationResult(stationName);
        result.sum = item.sum;
        result.count = item.count;
        result.max = item.maximum;
        result.min = item.minimum;
        resultMap.put(stationName, result);
      }

      return resultMap;
    }
  }

  private void processLine(byte[] line, StationTableItem[] items, List<Integer> occupiedIndexes) {
    // Find semicolon
    int semicolonPos = 0;
    while (semicolonPos < line.length && line[semicolonPos] != ';') {
      semicolonPos++;
    }
    if (semicolonPos >= line.length) return;

    // Extract name
    byte[] name = Arrays.copyOfRange(line, 0, semicolonPos);
    int hash = hashFnv(name);

    // Parse temperature
    long temp = 0;
    boolean negative = false;
    int pos = semicolonPos + 1;

    while (pos < line.length && line[pos] != '\n' && line[pos] != '\r') {
      byte b = line[pos];
      if (b == '-') {
        negative = true;
      } else if (b != '.') {
        temp = temp * 10 + (b - '0');
      }
      pos++;
    }

    if (negative) {
      temp = -temp;
    }

    // Linear probe
    int index = hash & TABLE_MASK;
    while (true) {
      StationTableItem item = items[index];

      if (!item.occupied) {
        // New entry
        item.name = name;
        item.hash = hash;
        item.sum = temp;
        item.count = 1;
        item.maximum = temp;
        item.minimum = temp;
        item.occupied = true;
        occupiedIndexes.add(index);
        break;
      } else if (Arrays.equals(item.name, name)) {
        // Update existing entry
        item.sum += temp;
        item.count++;
        if (temp > item.maximum) item.maximum = temp;
        if (temp < item.minimum) item.minimum = temp;
        break;
      }

      // Collision - linear probe
      index = (index + 1) & TABLE_MASK;
    }
  }

  // FNV-1a hash function like Go uses
  private int hashFnv(byte[] data) {
    int hash = 0x811c9dc5;
    for (byte b : data) {
      hash ^= (b & 0xff);
      hash *= 0x01000193;
    }
    return hash;
  }

  private static class StationTableItem {
    byte[] name;
    int hash;
    long sum;
    long count;
    long maximum;
    long minimum;
    boolean occupied;

    StationTableItem() {
      this.maximum = Long.MIN_VALUE;
      this.minimum = Long.MAX_VALUE;
    }
  }
}
