package com.onebillion.strategies;

import com.onebillion.result.Color;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
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
import java.util.stream.Collectors;

/**
 * Strategy to produce byte-range chunks for a file so work can be distributed across available PROCESSORS.
 *
 * <p>Chunks are non-overlapping and together cover the entire file. The file is divided into {@code
 * PROCESSORS} parts; the last chunk absorbs any remainder when the file size is not evenly divisible.
 */
public class ChunkReadStrategy {
    /**
     * Number of parallel chunks to produce, based on available CPU PROCESSORS.
     */
    private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

    private final String filepath;

    public ChunkReadStrategy(String filePath) {
        this.filepath = filePath;
    }

    public List<StationResult> runPlan(ChunkReader chunkReader, LineReader lineReader) throws IOException, ExecutionException, InterruptedException {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures =
                    produceChunks(filepath).stream()
                            .map(chunk -> executor.submit(() -> chunkReader.processChunk(chunk, lineReader)))
                            .toList();
            return createResults(futures);
        }
    }

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
    private List<Chunk> produceChunks(String filepath) throws IOException {
        var path = Paths.get(filepath);
        long fileSize = Files.size(path);
        long chunkSize = fileSize / PROCESSORS;

        List<Chunk> chunks = new ArrayList<>(PROCESSORS);
        try (var raf = new RandomAccessFile(filepath, "r")) {
            long lastEnd = 0;
            for (int i = 0; i < PROCESSORS; i++) {
                long start = lastEnd;
                long end = start + chunkSize;

                while (end < fileSize) {
                    raf.seek(end + 1);
                    if (raf.readByte() == '\n') {
                        end = end + 1;
                        break;
                    }
                    end++;
                }

                lastEnd = end;
                chunks.add(new Chunk(start, end, i == 0, i == PROCESSORS - 1, path));
            }
        }
        return chunks;
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


    @FunctionalInterface
    public interface ChunkReader {
        ChunkResult processChunk(Path path, Chunk chunk, LineReader reader) throws IOException;
    }

    static abstract class NioBufferReader {
        ChunkResult processChunk(Chunk chunk, LineReader reader, @NotNull ByteBuffer buffer) throws IOException {
            var lineBuf = new LineBuffer(reader);
            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                lineBuf.process(b);
            }
            return new ChunkResult(reader.collect(), lineBuf.getFilled());
        }
    }

    public static class StandardBufferedReader implements ChunkReader {
        @Override
        public ChunkResult processChunk(Path path, Chunk chunk, LineReader reader) throws IOException {
            try (var buffReader = Files.newBufferedReader(path)) {
                var buffer = new char[1024 * 1024];
                var lineBuff = new LineBuffer(reader);
                int bytesRead;

                while ((bytesRead = buffReader.read(buffer)) != -1) {
                    for (int i = 0; i < bytesRead; i++) {
                        lineBuff.process((byte) buffer[i]);
                    }
                }
                return new ChunkResult(reader.collect(), lineBuff.getFilled());
            }
        }
    }

    public static class MemoryMappedBufferedReader extends NioBufferReader implements ChunkReader {
        @Override
        public ChunkResult processChunk(Path path, Chunk chunk, LineReader reader) throws IOException {
            try (var raf = new RandomAccessFile(path.toFile(), "r");
                 var channel = raf.getChannel()
            ) {
                int size = (int) (chunk.end() - chunk.start());
                var buffer = channel.map(FileChannel.MapMode.READ_ONLY, chunk.start(), size);
                return processChunk(chunk, reader, buffer);
            }
        }
    }

    public static class ByteBufferedReader extends NioBufferReader implements ChunkReader {

        @Override
        public ChunkResult processChunk(Path path, Chunk chunk, LineReader reader) throws IOException {
            try (var raf = new RandomAccessFile(path.toFile(), "r");
                 var channel = raf.getChannel()
            ) {
                int size = (int) (chunk.end() - chunk.start());
                var buffer = java.nio.ByteBuffer.allocateDirect(size);
                channel.read(buffer, chunk.start());
                buffer.flip();
                return processChunk(chunk, reader, buffer);
            }
        }
    }

    public static class ArenaReader implements ChunkReader {
        @Override
        public ChunkResult processChunk(Path path, Chunk chunk, LineReader reader) throws IOException {
            try (var arena = Arena.ofConfined();
                 var channel = FileChannel.open(path)
            ) {
                long size = chunk.end() - chunk.start();
                var segment = channel.map(FileChannel.MapMode.READ_ONLY, chunk.start(), size, arena);
                var lineBuf = new LineBuffer(reader);
                long pos = 0;
                while (pos < size) {
                    byte b = segment.get(ValueLayout.JAVA_BYTE, pos);
                    pos++;
                    lineBuf.process(b);
                }
                return new ChunkResult(reader.collect(), lineBuf.getFilled());
            }
        }
    }

    public record Chunk(long start, long end, boolean isStart, boolean isEnd, Path path) {
    }

    public record ChunkResult(Map<String, StationResult> results, int rowCount) {
    }
}