package com.onebillion.strategies;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

public class LinearProbing implements LineReader {
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

    @Override
    public Map<String, StationResult> collect() {
        return StationTableItem.toResultMap(this.table, this.occupied);
    }

    @Override
    public void readLine(byte[] lineBytes, int start, int end) {
        int semicolonPos = getSemiColonIndex(lineBytes, start, end);
        if (semicolonPos == -1) throw new IllegalArgumentException("Invalid input: no semicolon found");

        int nameLen = semicolonPos - start;
        int hash = HashUtils.hashFnvDirect(lineBytes, start, nameLen);

        long temp = getTemp(semicolonPos, lineBytes, end);
        probe(lineBytes, start, nameLen, hash, temp);
    }

}