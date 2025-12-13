package com.onebillion.strategies;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class LinearProbing implements LineReader {
    private final int mask;
    private final StationTableItem[] table;
    private int occupied;

    public LinearProbing(int tableSize) {
        this.mask = tableSize - 1;
        this.table = new StationTableItem[tableSize];
        this.occupied = 0;
    }

    private static boolean arrayEquals(byte[] data, byte @NotNull [] array, int length, int offset) {
        if (length != array.length) return false;

        for (int i = 0; i < length; i++) {
            if (data[offset + i] != array[i]) return false;
        }
        return true;
    }

    public void parseAndProbe(byte[] nameByte, int nameLen) {
        int semicolonPos = -1;
        for (int i = 0; i < nameLen; i++) {
            if (nameByte[i] == ';') {
                semicolonPos = i;
                break;
            }
        }

        if (semicolonPos == -1) throw new IllegalArgumentException("Invalid input: no semicolon found");
        int hash = hashFnvDirect(nameByte, semicolonPos);

        boolean negative = false;
        long temp = 0;

        for (int i = semicolonPos + 1; i < nameLen; i++) {
            byte b = nameByte[i];
            if (b == '-') {
                negative = true;
            } else if (b != '.') {
                temp = temp * 10 + (b - '0');
            }
        }

        if (negative) {
            temp = -temp;
        }

        probe(nameByte, 0, semicolonPos, hash, temp);
    }

    public void probe(byte[] data, int nameOffset, int nameLen, int hash, long temp) {
        int index = hash & mask;
        while (true) {
            var item = table[index];
            if (item == null) {
                createNewItem(data, nameOffset, nameLen, hash, temp, index);
                return;
            }
            if (item.hash == hash && arrayEquals(data, item.name, nameLen, nameOffset)) {
                item.update(temp);
                return;
            }
            index = (index + 1) & mask;
        }
    }

    private void createNewItem(
            byte[] data, int nameOffset, int nameLen, int hash, long temp, int index) {
        byte[] name = new byte[nameLen];
        System.arraycopy(data, nameOffset, name, 0, nameLen);
        table[index] = new StationTableItem(name, temp, hash);
        this.occupied++;
    }

    private int hashFnvDirect(byte[] nameData, int length) {
        int hash = 0x811c9dc5;
        for (int i = 0; i < length; i++) {
            byte b = nameData[i];
            hash ^= (b & 0xff);
            hash *= 0x01000193;
        }
        return hash;
    }

    @Override
    public Map<String, StationResult> collect() {
        return Arrays.stream(table)
                .filter(Objects::nonNull)
                .collect(
                        Collectors.toMap(
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

    @Override
    public void readLine(byte[] lineBytes, int end) {
        parseAndProbe(lineBytes, end);
    }

    static class StationTableItem {
        byte[] name;
        int hash;
        long sum;
        long count;
        long maximum;
        long minimum;

        StationTableItem(byte[] name, long val, int hash) {
            this.name = name;
            this.sum = val;
            this.count = 1;
            this.maximum = val;
            this.minimum = val;
            this.hash = hash;
        }

        void update(long temp) {
            this.sum += temp;
            this.count++;
            if (temp > this.maximum) this.maximum = temp;
            if (temp < this.minimum) this.minimum = temp;
        }
    }
}