package com.onebillion;

import com.onebillion.strategies.BatchStrategy;
import com.onebillion.strategies.MCMPStrategy;
import com.onebillion.strategies.StationResult;
import com.onebillion.strategies.Strategy;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class Main {
  private static final String COLOR_RESET = "\033[0m";
  private static final String COLOR_RED = "\033[31m";
  private static final String COLOR_GREEN = "\033[32m";
  private static final String COLOR_YELLOW = "\033[33m";
  private static final String COLOR_BLUE = "\033[34m";
  private static final String COLOR_CYAN = "\033[36m";
  private static final String COLOR_BOLD = "\033[1m";

  static class BenchmarkResult {
    String strategyName;
    long executionTimeMs;
    long memoryUsedMB;
    int resultCount;
    boolean success;
    Exception error;

    BenchmarkResult(String strategyName) {
      this.strategyName = strategyName;
      this.success = false;
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

  public static void main(String[] args) {
    System.out.println(
        COLOR_BOLD + COLOR_CYAN + "=== One Billion Row Challenge - Benchmark ===" + COLOR_RESET);
    System.out.println();

    String dataFile = getDataFile(args);

    List<StrategyWrapper> strategies =
        List.of(
            new StrategyWrapper("MCMP Strategy", new MCMPStrategy()),
            new StrategyWrapper("Batch Strategy", new BatchStrategy()));

    List<BenchmarkResult> results = new ArrayList<>();

    for (StrategyWrapper s : strategies) {
      System.out.printf("%s⏱️  Running: %s%s%n", COLOR_YELLOW, s.name, COLOR_RESET);
      BenchmarkResult result = benchmarkStrategy(s.name, s.strategy, dataFile);
      results.add(result);

      if (result.success) {
        System.out.printf(
            "%s✓ Completed in: %s%s%n%n",
            COLOR_GREEN, formatDuration(result.executionTimeMs), COLOR_RESET);
      } else {
        System.out.printf(
            "%s✗ Failed: %s%s%n%n", COLOR_RED, result.error.getMessage(), COLOR_RESET);
      }
    }

    printSummary(results);
  }

  private static BenchmarkResult benchmarkStrategy(
      String name, Strategy strategy, String filePath) {
    BenchmarkResult result = new BenchmarkResult(name);
    System.gc();
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    Runtime runtime = Runtime.getRuntime();
    long memoryBefore = runtime.totalMemory() - runtime.freeMemory();

    // Start timing
    long startTime = System.currentTimeMillis();

    // Execute strategy
    List<StationResult> stationResults = null;
    try {
      stationResults = strategy.Analyze(filePath);
      result.success = true;
    } catch (Exception e) {
      result.error = e;
      result.success = false;
    }

    // End timing
    long executionTime = System.currentTimeMillis() - startTime;

    // Get memory stats after
    long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
    long memoryUsed = (memoryAfter - memoryBefore) / (1024 * 1024); // Convert to MB

    result.executionTimeMs = executionTime;
    result.memoryUsedMB = memoryUsed;
    result.resultCount = stationResults != null ? stationResults.size() : 0;
    return result;
  }

  private static void printSummary(List<BenchmarkResult> results) {
    System.out.println(COLOR_BOLD + COLOR_CYAN + "=== Performance Summary ===" + COLOR_RESET);
    System.out.println();

    if (results.isEmpty()) {
      System.out.println("No results to display");
      return;
    }

    BenchmarkResult fastest = null;
    for (BenchmarkResult result : results) {
      if (result.success && (fastest == null || result.executionTimeMs < fastest.executionTimeMs)) {
        fastest = result;
      }
    }

    AsciiTable table = new AsciiTable();
    table.addRule();
    table.addRow(
        COLOR_BOLD + COLOR_CYAN + "STRATEGY" + COLOR_RESET,
        COLOR_BOLD + COLOR_CYAN + "TIME" + COLOR_RESET,
        COLOR_BOLD + COLOR_CYAN + "MEMORY (MB)" + COLOR_RESET,
        COLOR_BOLD + COLOR_CYAN + "RESULTS" + COLOR_RESET,
        COLOR_BOLD + COLOR_CYAN + "STATUS" + COLOR_RESET);
    table.addRule();

    for (BenchmarkResult result : results) {
      String timeStr = formatDuration(result.executionTimeMs);
      String statusStr;
      String rowColor;

      if (result.success) {
        if (fastest != null && result.strategyName.equals(fastest.strategyName)) {
          statusStr = COLOR_GREEN + "✓ FASTEST" + COLOR_RESET;
          rowColor = COLOR_GREEN;
        } else {
          statusStr = "✓";
          rowColor = "";
        }
      } else {
        statusStr = COLOR_RED + "✗ FAILED" + COLOR_RESET;
        rowColor = COLOR_RED;
      }

      table.addRow(
          rowColor + result.strategyName + COLOR_RESET,
          timeStr,
          String.valueOf(result.memoryUsedMB),
          String.valueOf(result.resultCount),
          statusStr);

      if (result.error != null) {
        table.addRow(
            COLOR_RED + "  Error: " + result.error.getMessage() + COLOR_RESET, "", "", "", "");
      }
      table.addRule();
    }

    table.getRenderer().setCWC(new CWC_LongestLine());
    System.out.println(table.render());

    // Print comparison if multiple successful results
    int successfulResults = 0;
    for (BenchmarkResult r : results) {
      if (r.success) {
        successfulResults++;
      }
    }

    if (successfulResults > 1 && fastest != null) {
      System.out.println();
      System.out.println(
          COLOR_BOLD + COLOR_CYAN + "Speed Comparison (relative to fastest):" + COLOR_RESET);
      for (BenchmarkResult result : results) {
        if (result.success && !result.strategyName.equals(fastest.strategyName)) {
          double ratio = (double) result.executionTimeMs / (double) fastest.executionTimeMs;
          System.out.printf(
              "  %s is %.2fx slower than %s%n", result.strategyName, ratio, fastest.strategyName);
        }
      }
    }
  }

  private static String formatDuration(long milliseconds) {
    if (milliseconds < 1000) {
      return String.format("%d ms", milliseconds);
    }
    double seconds = milliseconds / 1000.0;
    if (seconds < 60) {
      return String.format("%.2f s", seconds);
    }
    double minutes = seconds / 60.0;
    return String.format("%.2f min", minutes);
  }

  private static String getDataFile(String @NotNull [] args) {
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
    var defaultPath = Paths.get("../data/measurements.txt");
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
}
