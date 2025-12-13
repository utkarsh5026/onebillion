package com.onebillion;

import static com.onebillion.result.Color.*;

import com.onebillion.result.BenchmarkResult;
import com.onebillion.result.StrategyRunner;
import com.onebillion.strategies.ReadingStrategies.*;
import com.onebillion.strategies.Strategy;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Main {

  public static void main(String[] args) {
    System.out.println(
        COLOR_BOLD + COLOR_CYAN + "=== One Billion Row Challenge - Benchmark ===" + COLOR_RESET);
    System.out.println();

    File resultsDir = new File("results");
    if (!resultsDir.exists()) {
      if (resultsDir.mkdirs()) {
        System.out.println(COLOR_BLUE + "Created results directory" + COLOR_RESET);
      }
      System.out.println(COLOR_BLUE + "Created results directory" + COLOR_RESET);
    }

    boolean printResults = args.length > 0 && args[0].equals("--print-results");
    String[] fileArgs =
        printResults && args.length > 1
            ? new String[] {args[1]}
            : printResults ? new String[0] : args;
    String dataFile = getDataFile(fileArgs);

    List<StrategyWrapper> strategies =
        List.of(
            // StandardBufferedReader combinations
            new StrategyWrapper("Standard-HashTable", new StandardBufferedHashTableStrategy()),
            new StrategyWrapper(
                "Standard-LinearProbing", new StandardBufferedLinearProbingStrategy()),

            // MemoryMappedBufferedReader combinations
            new StrategyWrapper("MemoryMapped-HashTable", new MemoryMappedHashTableStrategy()),
            new StrategyWrapper(
                "MemoryMapped-LinearProbing", new MemoryMappedLinearProbingStrategy()),

            // ByteBufferedReader combinations
            new StrategyWrapper("ByteBuffered-HashTable", new ByteBufferedHashTableStrategy()),
            new StrategyWrapper(
                "ByteBuffered-LinearProbing", new ByteBufferedLinearProbingStrategy()),

            // ArenaReader combinations
            new StrategyWrapper("Arena-HashTable", new ArenaHashTableStrategy()),
            new StrategyWrapper("Arena-LinearProbing", new ArenaLinearProbingStrategy()));

    List<BenchmarkResult> results = new ArrayList<>();

    for (StrategyWrapper s : strategies) {
      System.out.printf("%s⏱️  Running: %s%s%n", COLOR_YELLOW, s.name, COLOR_RESET);
      var result = StrategyRunner.benchmarkStrategy(s.name, s.strategy, dataFile, printResults);
      results.add(result);
      result.printExecutionResult();
    }

    BenchmarkResult.printSummary(results);
  }

  private static String getDataFile(String[] args) {
    if (args.length > 0) {
      File file = new File(args[0]);
      if (file.exists()) {
        System.out.println(COLOR_BLUE + "Using data file:" + COLOR_RESET + " " + args[0]);
        System.out.println();
        return args[0];
      }
      System.out.println(
          COLOR_YELLOW
              + "Warning: File '"
              + args[0]
              + "' not found, searching for alternatives..."
              + COLOR_RESET);
    }

    var dataPath = getDataPath().toAbsolutePath().toString();
    System.out.println(COLOR_BLUE + "Using default data file:" + COLOR_RESET + " " + dataPath);
    System.out.println();
    return dataPath;
  }

  private static Path getDataPath() {
    var defaultPath = Paths.get("../data/measurements-10m.txt");
    try (var paths = Files.list(Paths.get("../data"))) {
      return paths
          .filter(p -> p.getFileName().toString().startsWith("measurements-"))
          .filter(p -> p.getFileName().toString().endsWith(".txt"))
          .min(
              (p1, p2) -> {
                try {
                  return Files.getLastModifiedTime(p2).compareTo(Files.getLastModifiedTime(p1));
                } catch (Exception e) {
                  return 0;
                }
              })
          .orElse(defaultPath);
    } catch (Exception e) {
      return defaultPath;
    }
  }

  static class StrategyWrapper {
    String name;
    Strategy strategy;

    StrategyWrapper(String name, Strategy strategy) {
      this.name = name;
      this.strategy = strategy;
    }
  }
}
