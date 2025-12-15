package com.onebillion.strategies;

import com.onebillion.result.Color;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Zero-copy strategy that uses memory-mapped segments for maximum performance. This strategy
 * bypasses traditional buffering and directly parses data from memory segments.
 */
public class ZeroCopyStrategy implements Strategy {
  private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();
  private final String filepath;

  public ZeroCopyStrategy(String filepath) {
    this.filepath = filepath;
  }

  @Override
  public List<StationResult> Analyze(String filepath) throws IOException {
    try (var executor = Executors.newFixedThreadPool(PROCESSORS)) {
      var chunks = produceChunks(filepath);
      var reader = new ZeroCopyReader();

      var futures =
          chunks.stream().map(chunk -> executor.submit(() -> reader.processChunk(chunk))).toList();

      return createResults(futures);
    }
  }

  private List<Chunk> produceChunks(String filepath) throws IOException {
    var path = Paths.get(filepath);
    long fileSize = Files.size(path);
    long chunkSize = fileSize / PROCESSORS;

    List<Chunk> chunks = new ArrayList<>(PROCESSORS);
    try (var raf = new RandomAccessFile(filepath, "r")) {
      long lastEnd = 0;
      for (int i = 0; i < PROCESSORS; i++) {
        long start = lastEnd;
        long end = Math.min(start + chunkSize, fileSize);

        // Find the next newline after the calculated end position
        while (end < fileSize) {
          raf.seek(end);
          if (raf.readByte() == '\n') {
            break;
          }
          end++;
        }

        // Move past the newline for the next chunk's start
        if (end < fileSize) {
          end++;
        }

        lastEnd = end;
        chunks.add(new Chunk(filepath, start, end));
      }
    }
    return chunks;
  }

  private List<StationResult> createResults(List<Future<ChunkResult>> futures) {
    var results =
        futures.stream()
            .map(
                f -> {
                  try {
                    return f.get();
                  } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Error in thread execution: " + e.getMessage(), e);
                  }
                })
            .toList();

    int totalRowsRead = results.stream().mapToInt(ChunkResult::rowCount).sum();
    System.out.println(
        Color.COLOR_CYAN
            + "Read "
            + Color.COLOR_BOLD
            + totalRowsRead
            + Color.COLOR_RESET
            + Color.COLOR_CYAN
            + " rows"
            + Color.COLOR_RESET);

    var resultMaps = results.stream().map(ChunkResult::results).toList();
    return mergeMaps(resultMaps).values().stream().toList();
  }

  private Map<String, StationResult> mergeMaps(List<Map<String, StationResult>> maps) {
    return maps.stream()
        .flatMap(map -> map.entrySet().stream())
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (existing, newResult) -> {
                  existing.merge(newResult);
                  return existing;
                }));
  }

  public record Chunk(String filepath, long start, long end) {}

  public record ChunkResult(Map<String, StationResult> results, int rowCount) {}
}
