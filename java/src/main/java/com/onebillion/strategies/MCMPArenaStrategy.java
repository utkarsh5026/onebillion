package com.onebillion.strategies;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.jetbrains.annotations.NotNull;

/**
 * Ultra-optimized MCMP strategy using Arena and MemorySegment for zero-copy, off-heap memory
 * operations - similar to Go's performance
 */
public class MCMPArenaStrategy extends ChunkReadStrategy implements Strategy {
  private static final int TABLE_SIZE = 131072; // 2^17

  @Override
  public List<StationResult> Analyze(String filepath) throws IOException {

    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = launchWorkers(filepath, executor);
      return createResults(futures);
    }
  }

  private List<Future<ChunkResult>> launchWorkers(String path, ExecutorService executor)
      throws IOException {
    var chunks = produceChunks(path);
    List<Future<ChunkResult>> futures = new ArrayList<>(chunks.size());

    chunks.forEach((chunk) -> futures.add(executor.submit(() -> processChunk(chunk))));
    return futures;
  }

  @Override
  ChunkResult processChunk(@NotNull Chunk chunk) throws IOException {
    try (var arena = Arena.ofConfined();
        var channel = FileChannel.open(chunk.path(), StandardOpenOption.READ)) {

      long start = chunk.start();
      if (!chunk.isStart()) {
        start = findNextLineStart(channel, chunk.start());
      }

      long size = chunk.end() - start;
      MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, start, size, arena);
      return processSegment(segment, chunk.isEnd());
    }
  }

  private long findNextLineStart(FileChannel channel, long pos) throws IOException {
    try (Arena tempArena = Arena.ofConfined()) {
      MemorySegment temp = tempArena.allocate(128);
      channel.read(temp.asByteBuffer(), pos - 1);

      if (temp.get(ValueLayout.JAVA_BYTE, 0) == '\n') {
        return pos;
      }

      for (int i = 1; i < 128; i++) {
        if (temp.get(ValueLayout.JAVA_BYTE, i) == '\n') {
          return pos + i;
        }
      }
      return pos;
    }
  }

  private ChunkResult processSegment(MemorySegment segment, boolean isLast) {
    var probe = new LinearProbing(TABLE_SIZE);
    long size = segment.byteSize();
    long pos = 0;
    int count = 0;

    while (pos < size) {
      long semicolonPos = pos;
      while (semicolonPos < size && segment.get(ValueLayout.JAVA_BYTE, semicolonPos) != ';') {
        semicolonPos++;
      }

      if (semicolonPos >= size) break;

      int nameLen = (int) (semicolonPos - pos);
      byte[] name = extractName(segment, pos, nameLen);
      int hash = hashFnvDirect(segment, pos, nameLen);

      long tempPos = semicolonPos + 1;
      long temp = 0;
      boolean negative = false;

      while (tempPos < size) {
        byte b = segment.get(ValueLayout.JAVA_BYTE, tempPos);
        if (b == '\n' || b == '\r') {
          break;
        }
        if (b == '-') {
          negative = true;
        } else if (b != '.') {
          temp = temp * 10 + (b - '0');
        }
        tempPos++;
      }

      if (tempPos >= size && !isLast) {
        break;
      }

      if (negative) {
        temp = -temp;
      }

      probe.probe(name, hash, temp);
      count++;

      pos = tempPos + 1;
    }

    System.out.println("Processed " + count + " lines in chunk.");
    return new ChunkResult(probe.toMap(), count);
  }

  private int hashFnvDirect(MemorySegment segment, long offset, int length) {
    int hash = 0x811c9dc5;
    for (int i = 0; i < length; i++) {
      byte b = segment.get(ValueLayout.JAVA_BYTE, offset + i);
      hash ^= (b & 0xff);
      hash *= 0x01000193;
    }
    return hash;
  }

  private byte[] extractName(MemorySegment segment, long offset, int length) {
    byte[] name = new byte[length];
    for (int i = 0; i < length; i++) {
      name[i] = segment.get(ValueLayout.JAVA_BYTE, offset + i);
    }
    return name;
  }
}
