package com.onebillion.strategies;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        if (fileSize > 100_000_000) {
            return analyzeWithMemoryMapping(filepath, fileSize);
        } else {
            return analyzeWithBufferedReader(filepath);
        }
    }

    private List<StationResult> analyzeWithMemoryMapping(String filepath, long fileSize)
            throws IOException, ExecutionException, InterruptedException {

        long chunkSize = Math.max(fileSize / workerCount, 1024 * 1024); // At least 1MB per chunk
        List<Future<ChunkResult>> futures = new ArrayList<>(workerCount);

        try (RandomAccessFile file = new RandomAccessFile(filepath, "r");
             FileChannel channel = file.getChannel()) {

            for (int i = 0; i < workerCount; i++) {
                long start = i * chunkSize;
                if (start >= fileSize) break;

                long end = Math.min(start + chunkSize, fileSize);
                var isLastChunk = (i == workerCount - 1) || (end >= fileSize);

                long mappingEnd = isLastChunk ? end : Math.min(end + 128, fileSize);
                long size = mappingEnd - start;
                long chunkBoundary = end - start;

                var buffer = channel.map(FileChannel.MapMode.READ_ONLY, start, size);

                if (start > 0) {
                    file.seek(start - 1);
                    var prevByte = file.readByte();

                    if (prevByte != '\n') {
                        while (buffer.position() < buffer.limit() && buffer.get() != '\n') {
                        }
                    }
                }

                final int actualStartPos = buffer.position();

                futures.add(
                        executor.submit(
                                () -> processChunk(buffer, actualStartPos, chunkBoundary, isLastChunk)));
            }

            return mergeChunkResults(futures);
        }
    }

    private ChunkResult processChunk(
            MappedByteBuffer buffer, int actualStartPos, long chunkBoundary, boolean isLastChunk) {
        Map<String, StationResult> results = new HashMap<>(EXPECTED_STATIONS);
        buffer.position(actualStartPos);

        byte[] lineBuffer = new byte[256];
        int linePos = 0;
        int count = 0;
        boolean pastBoundary = false;

        while (buffer.hasRemaining()) {
            var b = buffer.get();
            if (!pastBoundary && !isLastChunk && buffer.position() > chunkBoundary) {
                pastBoundary = true;
            }

            if (b == '\n' || b == '\r') {
                if (linePos > 0) {
                    processLine(lineBuffer, linePos, results);
                    count++;
                    linePos = 0;
                }

                if (pastBoundary) {
                    break;
                }
                continue;
            }

            if (linePos < lineBuffer.length) {
                lineBuffer[linePos++] = b;
            }
        }

        return new ChunkResult(results, count);
    }

    private void processLine(
            byte[] lineBuffer, int length, @NotNull Map<String, StationResult> results) {
        var station = LineParser.processLine(lineBuffer, length);
        results
                .computeIfAbsent(station.name(), k -> new StationResult(station.name()))
                .add(station.value());
    }

    private @NotNull List<StationResult> analyzeWithBufferedReader(String filepath)
            throws IOException, ExecutionException, InterruptedException {

        BlockingQueue<BatchHolder> queue = new ArrayBlockingQueue<>(workerCount * 4);
        var tasks = getCallables(queue);

        List<Future<Map<String, StationResult>>> futures = new ArrayList<>(tasks.size());
        for (Callable<Map<String, StationResult>> task : tasks) {
            futures.add(executor.submit(task));
        }

        readFileOptimized(filepath, queue);
        return mergeFutureResults(futures);
    }

    private List<StationResult> mergeFutureResults(@NotNull List<Future<Map<String, StationResult>>> futures)
            throws ExecutionException, InterruptedException {

        Map<String, StationResult> finalRes = new HashMap<>(EXPECTED_STATIONS);

        for (Future<Map<String, StationResult>> future : futures) {
            Map<String, StationResult> workerResult = future.get();
            mergeMaps(finalRes, workerResult);
        }

        return new ArrayList<>(finalRes.values());
    }

    private List<StationResult> mergeChunkResults(List<Future<ChunkResult>> futures)
            throws ExecutionException, InterruptedException {

        Map<String, StationResult> finalRes = new HashMap<>(EXPECTED_STATIONS);
        int totalRowsRead = 0;

        for (Future<ChunkResult> future : futures) {
            ChunkResult chunkResult = future.get();
            mergeMaps(finalRes, chunkResult.results());
            totalRowsRead += chunkResult.rowCount();
        }

        System.out.println("Read " + totalRowsRead + " rows");

        return new ArrayList<>(finalRes.values());
    }

    private void mergeMaps(Map<String, StationResult> target, Map<String, StationResult> source) {
        source.forEach((key, val) -> target.computeIfAbsent(key, StationResult::new).merge(val));
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
            int totalRowsRead = 0;
            String line;

            while ((line = reader.readLine()) != null) {
                Station station = LineParser.parseLineStandard(line);
                batch[batchIndex++] = station;
                totalRowsRead++;
                if (batchIndex >= batchSize) {
                    queue.put(new BatchHolder(batch, batchSize));
                    batch = new Station[batchSize]; // New batch array
                    batchIndex = 0;
                }
            }

            if (batchIndex > 0) {
                queue.put(new BatchHolder(batch, batchIndex));
            }

            System.out.println("Read " + totalRowsRead + " rows");
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

    // Holder class to avoid array copying
    private record BatchHolder(Station[] batch, int size) {
    }

    // Holder class for chunk results with row count
    private record ChunkResult(Map<String, StationResult> results, int rowCount) {
    }
}