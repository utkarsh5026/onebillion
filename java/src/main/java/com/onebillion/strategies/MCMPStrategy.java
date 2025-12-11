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

  private @NotNull List<Future<Map<String, StationResult>>> launchWorkers(
      long chunkSize, long fileSize, FileChannel channel, ExecutorService executor)
      throws IOException {
    List<Future<Map<String, StationResult>>> futures = new ArrayList<>(processors);
    for (int i = 0; i < processors; i++) {
      long start = (long) i * chunkSize;
      long end = (i == processors - 1) ? fileSize : start + chunkSize;
      long size = end - start;

      var buffer = channel.map(FileChannel.MapMode.READ_ONLY, start + 128, size);
      final int workerId = i;
      futures.add(executor.submit(() -> processChunk(buffer, workerId == 0)));
    }
    return futures;
  }

  private @NotNull List<Map<String, StationResult>> waitForResult(
      @NotNull List<Future<Map<String, StationResult>>> futures) {
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

  private @NotNull Map<String, StationResult> processChunk(MappedByteBuffer buffer, boolean isFirst) {
    Map<String, StationResult> results = new HashMap<>(100_000);
    if (!isFirst) {
      while (buffer.hasRemaining() && buffer.get() != '\n');
    }

    byte[] lineBuffer = new byte[128];
    int linePos = 0;

    while (buffer.hasRemaining()) {
      byte b = buffer.get();

      if ((b == '\n' || b == '\r') && linePos > 0) {
        processLine(lineBuffer, linePos, results);
        linePos = 0;
      } else {
        if (linePos < lineBuffer.length) {
          lineBuffer[linePos++] = b;
        }
      }
    }

    if (linePos > 0) {
      processLine(lineBuffer, linePos, results);
    }
    return results;
  }

  private void processLine(
      byte[] lineBuffer, int length, @NotNull Map<String, StationResult> results) {
    var station = LineParser.processLine(lineBuffer, length);
    results.computeIfAbsent(station.name(), k -> new StationResult()).add(station.value());
  }
}