package com.onebillion.strategies;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.jetbrains.annotations.NotNull;

public class MCMPStrategy implements Strategy {
  private final int processors = Runtime.getRuntime().availableProcessors();

  @Override
  public List<StationResult> Analyze(String filepath) throws IOException {
    var fileSize = Files.size(Paths.get(filepath));
    var chunkSize = fileSize / processors;

    try (var file = new RandomAccessFile(filepath, "r");
        var channel = file.getChannel();
        var executor = Executors.newVirtualThreadPerTaskExecutor()) {

      var futures = launchWorkers(chunkSize, fileSize, channel, executor);
      var results = waitForResult(futures);
      return MapUtils.mergeMaps(results).values().stream().toList();
    }
  }

  private @NotNull List<Future<Map<Integer, StationResult>>> launchWorkers(
      long chunkSize, long fileSize, FileChannel channel, ExecutorService executor)
      throws IOException {
    List<Future<Map<Integer, StationResult>>> futures = new ArrayList<>(processors);
    for (int i = 0; i < processors; i++) {
      long start = (long) i * chunkSize;
      long end = (i == processors - 1) ? fileSize : start + chunkSize;

      // Extend the chunk to read up to 128 bytes past the boundary to catch the last
      // complete line
      long extendedEnd = (i == processors - 1) ? fileSize : Math.min(end + 128, fileSize);
      long size = extendedEnd - start;

      var buffer = channel.map(FileChannel.MapMode.READ_ONLY, start, size);
      final int workerId = i;
      final boolean isLast = i == processors - 1;
      futures.add(executor.submit(() -> processChunk(buffer, workerId == 0, isLast)));
    }
    return futures;
  }

  private @NotNull List<Map<Integer, StationResult>> waitForResult(
      @NotNull List<Future<Map<Integer, StationResult>>> futures) {
    List<Map<Integer, StationResult>> results = new ArrayList<>();
    for (var future : futures) {
      try {
        results.add(future.get());
      } catch (Exception e) {
        throw new RuntimeException("Error in thread execution: " + e.getMessage(), e);
      }
    }
    return results;
  }

  private @NotNull Map<Integer, StationResult> processChunk(
      MappedByteBuffer buffer, boolean isFirst, boolean isLast) {
    Map<Integer, StationResult> results = new HashMap<>(100_000);
    if (!isFirst) {
      while (buffer.hasRemaining() && buffer.get() != '\n')
        ;
    }

    while (buffer.hasRemaining()) {
      int stringStart = buffer.position();

      // Read station name and compute its hash
      int hash = 1;
      byte b;
      while (buffer.hasRemaining() && (b = buffer.get()) != ';') {
        hash = 31 * hash + b;
      }

      // If we ran out of buffer before finding ';', this is an incomplete line - skip it
      if (!buffer.hasRemaining()) {
        break;
      }

      int nameLen = buffer.position() - stringStart - 1;

      // parse the numeric value
      int temp = 0;
      boolean negative = false;

      while (buffer.hasRemaining() && (b = buffer.get()) != '\n' && b != '\r') {
        if (b == '-') {
          negative = true;
          continue;
        }
        if (b == '.') {
          // Skip decimal point
          continue;
        }
        temp = temp * 10 + (b - '0');
      }

      // If we ran out of buffer before finding '\n', this is an incomplete line - skip it
      if (!buffer.hasRemaining() && !isLast) {
        break;
      }

      if (negative) {
        temp = -temp;
      }

      // Update StationResult in the map
      if (results.containsKey(hash)) {
        results.get(hash).add(temp);
      } else {
        int savedPos = buffer.position();
        byte[] nameBytes = new byte[nameLen];
        buffer.position(stringStart);
        buffer.get(nameBytes);
        buffer.position(savedPos);
        var stationResult = new StationResult(new String(nameBytes));
        stationResult.add(temp);
        results.put(hash, stationResult);
      }
    }
    return results;
  }
}
