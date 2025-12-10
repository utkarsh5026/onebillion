package com.onebillion.strategies;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class BatchProcessor {
    private static  final List<Station> POISON_PILL = Collections.emptyList();
    final int processors = Runtime.getRuntime().availableProcessors();
    ExecutorService executor = Executors.newFixedThreadPool(processors);


    List<StationResult> analyze(String filepath) throws ExecutionException, InterruptedException {
        BlockingQueue<List<Station>> queue = new ArrayBlockingQueue<>(processors * 2);
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

    private @NotNull List<Callable<Map<String, StationResult>>> getCallables(BlockingQueue<List<Station>> queue) {
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
                        localResults.putIfAbsent(station.name(), new StationResult());
                        localResults.get(station.name()).add((long)(station.value() * 10));
                    }
                }
                return localResults;
            });
        }
        return tasks;
    }

    private void readFile(String filepath, BlockingQueue<List<Station>> queue) {
        var path = Paths.get(filepath);
        try (var reader = Files.newBufferedReader(path)) {
            List<Station> batch = new ArrayList<>(100);
            String line;
            while ((line = reader.readLine()) != null) {
                var station = LineParser.parseLineStandard(line);
                batch.add(station);
                if (batch.size() >= 100) {
                    queue.put(new ArrayList<>(batch));
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                queue.put(batch);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
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