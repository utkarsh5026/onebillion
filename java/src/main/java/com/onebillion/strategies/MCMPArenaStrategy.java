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

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * Ultra-optimized MCMP strategy using Arena and MemorySegment for zero-copy,
 * off-heap memory operations - similar to
 * Go's performance
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
            return processSegment(segment);
        }
    }

    private long findNextLineStart(FileChannel channel, long pos) throws IOException {
        try (Arena tempArena = Arena.ofConfined()) {
            var temp = tempArena.allocate(128);
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

    @Contract("_, _ -> new")
    private @NotNull ChunkResult processSegment(@NotNull MemorySegment segment) {
        var probe = new LinearProbing(TABLE_SIZE);
        long size = segment.byteSize();
        long pos = 0;
        int count = 0;

        byte[] lineData = new byte[256];
        int lineOffset = 0;
        while (pos < size) {
            while (pos < size) {
                byte b = segment.get(ValueLayout.JAVA_BYTE, pos);
                pos++;
                if (b == '\n' || b == '\r') {
                    probe.parseAndProbe(lineData, lineOffset);
                    lineOffset = 0;
                    count++;
                    break;
                }

                if (lineOffset < lineData.length) {
                    lineData[lineOffset++] = b;
                }
            }
        }
        return new ChunkResult(probe.toMap(), count);
    }

}