package com.onebillion.strategies;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class BatchProcessor {
  private static final Station[] POISON_PILL = new Station[0];
  private static final int EXPECTED_STATIONS = 10000; // Pre-sizing hint
  private final int processors = Runtime.getRuntime().availableProcessors();
  private final int workerCount = processors * 2;
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

  List<StationResult> analyze(String filepath)
      throws ExecutionException, InterruptedException, IOException {
    long fileSize = Files.size(Paths.get(filepath));

    // Use memory-mapped I/O for better performance on large files
    if (fileSize > 100_000_000) { // 100MB threshold
      return analyzeWithMemoryMapping(filepath, fileSize);
    } else {
      return analyzeWithBufferedReader(filepath);
    }
  }

  private List<StationResult> analyzeWithMemoryMapping(String filepath, long fileSize)
      throws IOException, ExecutionException, InterruptedException {

    long chunkSize = Math.max(fileSize / workerCount, 1024 * 1024); // At least 1MB per chunk
    List<Future<Map<String, StationResult>>> futures = new ArrayList<>(workerCount);

    try (RandomAccessFile file = new RandomAccessFile(filepath, "r");
        FileChannel channel = file.getChannel()) {

      for (int i = 0; i < workerCount; i++) {
        long start = i * chunkSize;
        if (start >= fileSize) break;

        long end = Math.min(start + chunkSize, fileSize);
        long size = end - start;

        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, start, size);

        // Adjust start to next line boundary (except for first chunk)
        if (start > 0) {
          while (buffer.position() < buffer.limit() && buffer.get() != '\n') {
            // Skip to next newline
          }
        }

        final int startPos = buffer.position();
        final int finalI = i;

        futures.add(
            executor.submit(() -> processChunk(buffer, startPos, finalI < workerCount - 1)));
      }

      return mergeFutureResults(futures);
    }
  }

  private Map<String, StationResult> processChunk(
      MappedByteBuffer buffer, int startPos, boolean stopAtBoundary) {
    Map<String, StationResult> results = new HashMap<>(EXPECTED_STATIONS);
    buffer.position(startPos);

    byte[] lineBuffer = new byte[128]; // Most station names + value fit in 128 bytes
    int linePos = 0;

    while (buffer.hasRemaining()) {
      byte b = buffer.get();

      if (b == '\n' || b == '\r') {
        if (linePos > 0) {
          processLine(lineBuffer, linePos, results);
          linePos = 0;
        }
        // For boundary chunks, stop at the end
        if (stopAtBoundary && !buffer.hasRemaining()) {
          break;
        }
      } else {
        if (linePos < lineBuffer.length) {
          lineBuffer[linePos++] = b;
        }
      }
    }

    // Process last line if exists
    if (linePos > 0 && !stopAtBoundary) {
      processLine(lineBuffer, linePos, results);
    }

    return results;
  }

  private void processLine(byte[] lineBuffer, int length, Map<String, StationResult> results) {
    var station = LineParser.processLine(lineBuffer, length);
    results
        .computeIfAbsent(station.name(), k -> new StationResult(station.name()))
        .add(station.value());
  }

  private List<StationResult> analyzeWithBufferedReader(String filepath)
      throws IOException, ExecutionException, InterruptedException {

    BlockingQueue<BatchHolder> queue = new ArrayBlockingQueue<>(workerCount * 4);
    var tasks = getCallables(queue);
    var futures = submitTasks(tasks);

    readFileOptimized(filepath, queue);
    return mergeFutureResults(futures);
  }

  private List<StationResult> mergeFutureResults(List<Future<Map<String, StationResult>>> futures)
      throws ExecutionException, InterruptedException {

    Map<String, StationResult> finalRes = new HashMap<>(EXPECTED_STATIONS);

    for (Future<Map<String, StationResult>> future : futures) {
      Map<String, StationResult> workerResult = future.get();
      mergeMaps(finalRes, workerResult);
    }

    return new ArrayList<>(finalRes.values());
  }

  private void mergeMaps(Map<String, StationResult> target, Map<String, StationResult> source) {
    source.forEach((key, val) -> target.computeIfAbsent(key, k -> new StationResult(k)).merge(val));
  }

  private List<Callable<Map<String, StationResult>>> getCallables(
      BlockingQueue<BatchHolder> queue) {
    List<Callable<Map<String, StationResult>>> tasks = new ArrayList<>(workerCount);

    for (int i = 0; i < workerCount; i++) {
      tasks.add(
          () -> {
            Map<String, StationResult> localResults = new HashMap<>(EXPECTED_STATIONS);
            while (true) {
              BatchHolder holder = queue.take();
              if (holder.batch == POISON_PILL) {
                break;
              }

              int limit = holder.size;
              Station[] batch = holder.batch;
              for (int j = 0; j < limit; j++) {
                Station station = batch[j];
                localResults
                    .computeIfAbsent(station.name(), k -> new StationResult(station.name()))
                    .add(station.value());
              }
            }
            return localResults;
          });
    }
    return tasks;
  }

  private void readFileOptimized(String filepath, BlockingQueue<BatchHolder> queue)
      throws IOException {
    Path path = Paths.get(filepath);
    int batchSize = 20000; // Increased batch size for better throughput

    try (var reader = Files.newBufferedReader(path)) {
      Station[] batch = new Station[batchSize];
      int batchIndex = 0;
      String line;

      while ((line = reader.readLine()) != null) {
        Station station = LineParser.parseLineStandard(line);
        batch[batchIndex++] = station;
        if (batchIndex >= batchSize) {
          queue.put(new BatchHolder(batch, batchSize));
          batch = new Station[batchSize]; // New batch array
          batchIndex = 0;
        }
      }

      if (batchIndex > 0) {
        queue.put(new BatchHolder(batch, batchIndex));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("File reading interrupted", e);
    } finally {
      try {
        for (int i = 0; i < workerCount; i++) {
          queue.put(new BatchHolder(POISON_PILL, 0));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private List<Future<Map<String, StationResult>>> submitTasks(
      List<Callable<Map<String, StationResult>>> tasks) {
    List<Future<Map<String, StationResult>>> futures = new ArrayList<>(tasks.size());
    for (Callable<Map<String, StationResult>> task : tasks) {
      futures.add(executor.submit(task));
    }
    return futures;
  }

  // Holder class to avoid array copying
  private record BatchHolder(Station[] batch, int size) {}
}
