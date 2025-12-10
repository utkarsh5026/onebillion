package com.onebillion.strategies;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class BatchProcessor {
    private static  final Station[] POISON_PILL = new Station[0];
    final int processors = Runtime.getRuntime().availableProcessors();
    ExecutorService executor = Executors.newFixedThreadPool(processors);


    List<StationResult> analyze(String filepath) throws ExecutionException, InterruptedException, IOException {
        BlockingQueue<Station[]> queue = new ArrayBlockingQueue<>(processors * 4);
        var tasks = getCallables(queue);
        var futures = submitTasks(tasks);

        readFile(filepath, queue);
        Map<String, StationResult> finalRes = new HashMap<>();

        for (Future<Map<String, StationResult>> future : futures) {
            Map<String, StationResult> workerResult = future.get(); // Waits for worker
            mergeMaps(finalRes, workerResult);
        }

        return new ArrayList<>(finalRes.values());
    }

    private void mergeMaps(Map<String, StationResult> target, @NotNull Map<String, StationResult> source) {
        source.forEach((key, val) ->
                target.computeIfAbsent(key, k -> new StationResult()).merge(val)
        );
    }

    private @NotNull List<Callable<Map<String, StationResult>>> getCallables(BlockingQueue<Station[]> queue) {
        List<Callable<Map<String, StationResult>>> tasks = new ArrayList<>();

        for  (int i = 0; i < processors; i++) {
            tasks.add(() -> {
                Map<String, StationResult> localResults = new HashMap<>();
                while (true) {
                    var batch = queue.take();
                    if (batch == POISON_PILL) {
                        break;
                    }
                    for (var station : batch) {
                        Objects.requireNonNull(localResults.putIfAbsent(station.name(), new StationResult())).add(station.value());
                    }
                }
                return localResults;
            });
        }
        return tasks;
    }

    private void readFile(String filepath, BlockingQueue<Station[]> queue) throws IOException {
        var path = Paths.get(filepath);
        int batchSize = 10000;
        try (var reader = Files.newBufferedReader(path)) {
            Station[] batch = new Station[batchSize];
            int batchIndex = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                var station = LineParser.parseLineStandard(line);
                batch[batchIndex++] = station;
                if (batchIndex >= batchSize) {
                    queue.put(Arrays.copyOf(batch, batchIndex));
                    batchIndex = 0;
                }
            }

            if (batchIndex > 0) {
                queue.put(Arrays.copyOf(batch, batchIndex));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                for (int i = 0; i < processors; i++) {
                    queue.put(POISON_PILL);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private List<Future<Map<String, StationResult>>> submitTasks(List<Callable<Map<String, StationResult>>> tasks) {
        List<Future<Map<String, StationResult>>> futures = new ArrayList<>();
        for (Callable<Map<String, StationResult>> task : tasks) {
            futures.add(executor.submit(task));
        }
        return futures;
    }
}