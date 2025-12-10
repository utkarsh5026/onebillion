package com.onebillion.strategies;

import org.jetbrains.annotations.NotNull;

record Station(String name, int value) {}

public class LineParser {
    final static char  DELIMITER = ';';

    public static @NotNull Station parseLineStandard(@NotNull String line) {
        int commaIndex = line.indexOf(DELIMITER);
        String name = line.substring(0, commaIndex);
        int value = parseInt(line.substring(commaIndex + 1));
        return new Station(name, value);
    }

    private static  int parseInt(String line) {
        int result = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == DELIMITER) {
                continue;
            }
            char c = line.charAt(i);
            result = result * 10 + (c - '0');
        }
        return result;
    }
}