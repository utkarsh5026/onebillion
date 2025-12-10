package com.onebillion.strategies;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

record Station(String name, double value) {}

public class LineParser {

    @Contract("_ -> new")
    public static @NotNull Station parseLine(@NotNull String line) {
        String[] parts = line.split(",");
        String name = parts[0];
        double value = Double.parseDouble(parts[1]);
        return new Station(name, value);
    }

}