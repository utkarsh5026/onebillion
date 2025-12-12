package com.onebillion.strategies;

import com.onebillion.result.Color;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Strategy to produce byte-range chunks for a file so work can be distributed across available
 * processors.
 *
 * <p>Chunks are non-overlapping and together cover the entire file. The file is divided into {@code
 * processors} parts; the last chunk absorbs any remainder when the file size is not evenly
 * divisible.
 */
abstract class ChunkReadStrategy {
  /** Number of parallel chunks to produce, based on available CPU processors. */
  private final int processors = Runtime.getRuntime().availableProcessors();

  /**
   * Produce a list of {@link Chunk} objects that partition the file at the given filepath.
   *
   * <p>Each chunk contains an inclusive start byte and an exclusive end byte, along with flags
   * indicating whether it is the first or last chunk.
   *
   * @param filepath path to the file to split into chunks
   * @return list of {@link Chunk} describing byte ranges that cover the file
   * @throws IOException if the file size cannot be determined
   */
  protected List<Chunk> produceChunks(String filepath) throws IOException {
    var path = Paths.get(filepath);
    long fileSize = Files.size(path);
    long chunkSize = fileSize / processors;
    return IntStream.range(0, processors)
        .mapToObj(
            i -> {
              long start = (long) i * chunkSize;
              long end = (i == processors - 1) ? fileSize : start + chunkSize;

              boolean isStart = i == 0;
              boolean isEnd = i == processors - 1;
              return new Chunk(start, end, isStart, isEnd, path);
            })
        .toList();
  }

  protected List<StationResult> createResults(List<Future<ChunkResult>> futures) {
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

  abstract ChunkResult processChunk(Chunk chunk) throws IOException;

  record Chunk(long start, long end, boolean isStart, boolean isEnd, Path path) {}

  record ChunkResult(Map<String, StationResult> results, int rowCount) {}
}
