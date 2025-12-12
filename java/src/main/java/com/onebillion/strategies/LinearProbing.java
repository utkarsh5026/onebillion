package com.onebillion.strategies;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LinearProbing {
    private final int mask;
    private final StationTableItem[] table;
    private int occupied;

    public LinearProbing(int tableSize) {
        this.mask = tableSize - 1;
        this.table = new StationTableItem[tableSize];
        this.occupied = 0;
    }

    public void probe(byte[] name, int hash, long temp) {
        int index = hash & mask;
        while (true) {
            var item = table[index];
            if (item == null) {
                table[index] = new StationTableItem(name, temp);
                this.occupied++;
                return;
            }
            if (item.hash == hash && Arrays.equals(item.name, name)) {
                item.update(temp);
                return;
            }
            index = (index + 1) & mask;
        }
    }

    public Map<String, StationResult> toMap() {
        return Arrays.stream(table)
                .filter(item -> item != null)
                .collect(Collectors.toMap(
                        item -> new String(item.name),
                        item -> {
                            var result = new StationResult(new String(item.name));
                            result.sum = item.sum;
                            result.count = item.count;
                            result.max = item.maximum;
                            result.min = item.minimum;
                            return result;
                        },
                        (a, b) -> a,
                        () -> new HashMap<>(occupied)));
    }

    static class StationTableItem {
        byte[] name;
        int hash;
        long sum;
        long count;
        long maximum;
        long minimum;

        StationTableItem(byte[] name, long val) {
            this.name = name;
            this.sum = val;
            this.count = 1;
            this.maximum = val;
            this.minimum = val;
        }

        void update(long temp) {
            this.sum += temp;
            this.count++;
            if (temp > this.maximum)
                this.maximum = temp;
            if (temp < this.minimum)
                this.minimum = temp;
        }
    }
}