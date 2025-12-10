package com.onebillion.strategies;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class BatchStrategy implements Strategy {
    private final BatchProcessor processor;

    public BatchStrategy() {
        this.processor = new BatchProcessor();
    }

    @Override
    public List<StationResult> Analyze(String filepath) {
        try {
            return processor.analyze(filepath);
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to analyze file: " + e.getMessage(), e);
        }
    }
}
