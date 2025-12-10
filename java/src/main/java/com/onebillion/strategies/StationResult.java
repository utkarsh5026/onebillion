package com.onebillion.strategies;

import org.jetbrains.annotations.NotNull;

public class StationResult {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long sum = 0;
    long count = 0;

    void add(long value) {
        min = Math.min(min, value);
        max = Math.max(max, value);
        sum += value;
        count++;
    }

    void merge(@NotNull StationResult other) {
        min = Math.min(other.min, min);
        max = Math.max(max, other.max);
        sum += other.sum;
        count += other.count;
    }


    @Override
    public String toString() {
        double mean = count > 0 ? (double)sum/(double)count: 0;
        return String.format("min/mean/max = %.1f/%.1f/%.1f", min/10.0, mean, max/10.0);
    }
}