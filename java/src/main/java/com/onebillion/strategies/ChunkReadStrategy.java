package com.onebillion.strategies;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

import com.onebillion.result.Color;

/**
 * Strategy to produce byte-range chunks for a file so work can be distributed
 * across available
 * PROCESSORS.
 *
 * <p>
 * Chunks are non-overlapping and together cover the entire file. The file is
 * divided into {@code
 * PROCESSORS} parts; the last chunk absorbs any remainder when the file size is
 * not evenly
 * divisible.
 */
public class ChunkReadStrategy {
  /** Number of parallel chunks to produce, based on available CPU PROCESSORS. */
  private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

  /** Batch size for buffered reading operations. */
  private static final int BATCH_SIZE = 8192; // 8KB batch size

  private final String filepath;

  public ChunkReadStrategy(String filePath) {
    this.filepath = filePath;
  }

  public List<StationResult> runPlan(
      ChunkReader chunkReader, Supplier<LineReader> lineReaderSupplier)
      throws IOException, ExecutionException, InterruptedException {
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = produceChunks(filepath).stream()
          .map(
              chunk -> executor.submit(
                  () -> chunkReader.processChunk(
                      chunk.path(), chunk, lineReaderSupplier.get())))
          .toList();
      return createResults(futures);
    }
  }

  /**
   * Produce a list of {@link Chunk} objects that partition the file at the given
   * filepath.
   *
   * <p>
   * Each chunk contains an inclusive start byte and an exclusive end byte, along
   * with flags
   * indicating whether it is the first or last chunk.
   *
   * @param filepath path to the file to split into chunks
   * @return list of {@link Chunk} describing byte ranges that cover the file
   * @throws IOException if the file size cannot be determined
   */
  protected List<Chunk> produceChunks(String filepath) throws IOException {
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
          end++; // Skip the newline character
        }

        lastEnd = end;
        chunks.add(new Chunk(i, start, end, i == 0, i == PROCESSORS - 1, path));
      }
    }
    return chunks;
  }

  protected List<StationResult> createResults(@NotNull List<Future<ChunkResult>> futures) {
    var results = futures.stream()
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

  private Map<String, StationResult> mergeMaps(@NotNull List<Map<String, StationResult>> maps) {
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

  @FunctionalInterface
  public interface ChunkReader {
    ChunkResult processChunk(Path path, Chunk chunk, LineReader reader) throws IOException;
  }

  abstract static class NioBufferReader {
    ChunkResult processChunk(Chunk chunk, LineReader reader, @NotNull ByteBuffer buffer) {
      var lineBuf = new LineBuffer(reader);
      var batch = new byte[BATCH_SIZE];

      while (buffer.hasRemaining()) {
        int toRead = Math.min(buffer.remaining(), batch.length);
        buffer.get(batch, 0, toRead);
        lineBuf.processBuffer(batch, toRead);
      }
      lineBuf.flush();
      return new ChunkResult(reader.collect(), lineBuf.getFilled());
    }
  }

  public static class StandardBufferedReader implements ChunkReader {
    @Override
    public ChunkResult processChunk(Path path, Chunk chunk, LineReader reader) throws IOException {
      try (var raf = new RandomAccessFile(path.toFile(), "r")) {
        long size = chunk.end() - chunk.start();
        raf.seek(chunk.start());
        var buffer = new byte[(int) Math.min(size, 1024 * 1024)];
        var lineBuff = new LineBuffer(reader);
        long totalRead = 0;

        while (totalRead < size) {
          int toRead = (int) Math.min(buffer.length, size - totalRead);
          int bytesRead = raf.read(buffer, 0, toRead);
          if (bytesRead == -1)
            break;

          lineBuff.processBuffer(buffer, bytesRead);
          totalRead += bytesRead;
        }
        lineBuff.flush();
        return new ChunkResult(reader.collect(), lineBuff.getFilled());
      }
    }
  }

  public static class MemoryMappedBufferedReader extends NioBufferReader implements ChunkReader {
    @Override
    public ChunkResult processChunk(Path path, Chunk chunk, LineReader reader) throws IOException {
      try (var raf = new RandomAccessFile(path.toFile(), "r");
          var channel = raf.getChannel()) {
        int size = (int) (chunk.end() - chunk.start());
        var buffer = channel.map(FileChannel.MapMode.READ_ONLY, chunk.start(), size);
        return processChunk(chunk, reader, buffer);
      }
    }
  }

  public static class ByteBufferedReader implements ChunkReader {

    @Override
    public ChunkResult processChunk(Path path, Chunk chunk, LineReader reader) throws IOException {
      try (var raf = new RandomAccessFile(path.toFile(), "r");
          var channel = raf.getChannel()) {
        long chunkSize = chunk.end() - chunk.start();
        var buffer = java.nio.ByteBuffer.allocateDirect(BATCH_SIZE);
        var lineBuf = new LineBuffer(reader);
        var batch = new byte[BATCH_SIZE];
        long position = chunk.start();
        long remaining = chunkSize;

        while (remaining > 0) {
          buffer.clear();
          int toRead = (int) Math.min(BATCH_SIZE, remaining);
          buffer.limit(toRead);
          int bytesRead = channel.read(buffer, position);
          if (bytesRead == -1)
            break;

          buffer.flip();
          position += bytesRead;
          remaining -= bytesRead;

          while (buffer.hasRemaining()) {
            int batchSize = Math.min(buffer.remaining(), batch.length);
            buffer.get(batch, 0, batchSize);
            lineBuf.processBuffer(batch, batchSize);
          }
        }
        lineBuf.flush();
        return new ChunkResult(reader.collect(), lineBuf.getFilled());
      }
    }
  }

  public static class ArenaReader implements ChunkReader {
    @Override
    public ChunkResult processChunk(Path path, Chunk chunk, LineReader reader) throws IOException {
      try (var arena = Arena.ofConfined();
          var channel = FileChannel.open(path)) {
        long size = chunk.end() - chunk.start();
        var segment = channel.map(FileChannel.MapMode.READ_ONLY, chunk.start(), size, arena);
        var lineBuf = new LineBuffer(reader);
        var batch = new byte[BATCH_SIZE];
        long pos = 0;

        while (pos < size) {
          int toRead = (int) Math.min(batch.length, size - pos);
          // Bulk copy instead of byte-by-byte for much better performance
          MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, pos, batch, 0, toRead);
          lineBuf.processBuffer(batch, toRead);
          pos += toRead;
        }
        lineBuf.flush(); // Flush any remaining spillover
        return new ChunkResult(reader.collect(), lineBuf.getFilled());
      }
    }
  }

  public record Chunk(int index, long start, long end, boolean isStart, boolean isEnd, Path path) {
  }

  public record ChunkResult(Map<String, StationResult> results, int rowCount) {
  }
}
