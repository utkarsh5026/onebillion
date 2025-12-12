package com.onebillion.strategies;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class MCMPStrategy extends ChunkReadStrategy implements Strategy {

    @Override
    public List<StationResult> Analyze(String filepath) throws IOException {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = produceChunks(filepath).stream()
                    .map(chunk -> executor.submit(() -> processChunk(chunk)))
                    .toList();
            return createResults(futures);
        }
    }

    @Override
    ChunkResult processChunk(@NotNull Chunk chunk) throws IOException {
        try (var file = new RandomAccessFile(chunk.path().toString(), "r");
             var channel = file.getChannel()) {

            long extendedEnd = chunk.isEnd() ? chunk.end() : Math.min(chunk.end() + 128, Files.size(chunk.path()));
            long size = extendedEnd - chunk.start();

            var buffer = channel.map(FileChannel.MapMode.READ_ONLY, chunk.start(), size);
            var resultMap = processBuffer(buffer, chunk.isStart(), chunk.isEnd());
            int rowCount = (int) resultMap.values().stream().mapToLong(StationResult::getCount).sum();

            return new ChunkResult(resultMap, rowCount);
        }
    }

    private Map<String, StationResult> processBuffer(
            MappedByteBuffer buffer, boolean isFirst, boolean isLast) {
        Map<String, StationResult> results = new HashMap<>(512);

        if (!isFirst) {
            while (buffer.hasRemaining() && buffer.get() != '\n')
                ;
        }

        byte[] nameBuffer = new byte[128];

        while (buffer.hasRemaining()) {
            int hash = 1;
            byte b;
            int nameLen = 0;

            while (buffer.hasRemaining() && (b = buffer.get()) != ';') {
                hash = 31 * hash + b;
                if (nameLen < nameBuffer.length) {
                    nameBuffer[nameLen] = b;
                }
                nameLen++;
            }

            // If we ran out of buffer before finding ';', this is an incomplete line - skip
            // it
            if (!buffer.hasRemaining()) {
                break;
            }

            // parse the numeric value
            int temp = 0;
            boolean negative = false;

            while (buffer.hasRemaining() && (b = buffer.get()) != '\n' && b != '\r') {
                if (b == '-') {
                    negative = true;
                    continue;
                }
                if (b == '.') {
                    continue;
                }
                temp = temp * 10 + (b - '0');
            }

            // If we ran out of buffer before finding '\n', this is an incomplete line -
            // skip it
            if (!buffer.hasRemaining() && !isLast) {
                break;
            }

            if (negative) {
                temp = -temp;
            }

            // Update StationResult in the map
            // Only create String when we need to look up
            String stationName = new String(nameBuffer, 0, Math.min(nameLen, nameBuffer.length));
            StationResult result = results.get(stationName);
            if (result != null) {
                result.add(temp);
            } else {
                result = new StationResult(stationName);
                result.add(temp);
                results.put(stationName, result);
            }
        }
        return results;
    }
}