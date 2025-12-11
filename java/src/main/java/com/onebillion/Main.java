package com.onebillion;

import com.onebillion.strategies.BatchStrategy;
import com.onebillion.strategies.MCMPArenaStrategy;
import com.onebillion.strategies.MCMPOptimizedStrategy;
import com.onebillion.strategies.MCMPStrategy;
import com.onebillion.strategies.StationResult;
import com.onebillion.strategies.Strategy;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
  private static final String COLOR_RESET = "\033[0m";
  private static final String COLOR_RED = "\033[31m";
  private static final String COLOR_GREEN = "\033[32m";
  private static final String COLOR_YELLOW = "\033[33m";
  private static final String COLOR_BLUE = "\033[34m";
  private static final String COLOR_CYAN = "\033[36m";
  private static final String COLOR_BOLD = "\033[1m";

  public static void main(String[] args) {
    System.out.println(
        COLOR_BOLD + COLOR_CYAN + "=== One Billion Row Challenge - Benchmark ===" + COLOR_RESET);
    System.out.println();

    boolean printResults = args.length > 0 && args[0].equals("--print-results");
    String[] fileArgs = printResults && args.length > 1
        ? new String[] { args[1] }
        : printResults ? new String[0] : args;
    String dataFile = getDataFile(fileArgs);

    List<StrategyWrapper> strategies = List.of(
        new StrategyWrapper("MCMP Strategy", new MCMPStrategy()),
        new StrategyWrapper("MCMP Arena", new MCMPArenaStrategy()),
        new StrategyWrapper("MCMP Hash probing", new MCMPOptimizedStrategy()),
        new StrategyWrapper("Batch Strategy", new BatchStrategy()));

    List<BenchmarkResult> results = new ArrayList<>();

    for (StrategyWrapper s : strategies) {
      System.out.printf("%s⏱️  Running: %s%s%n", COLOR_YELLOW, s.name, COLOR_RESET);
      BenchmarkResult result = benchmarkStrategy(s.name, s.strategy, dataFile, printResults);
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
      String name, Strategy strategy, String filePath, boolean printResults) {
    BenchmarkResult result = new BenchmarkResult(name);
    System.gc();
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    Runtime runtime = Runtime.getRuntime();
    long memoryBefore = runtime.totalMemory() - runtime.freeMemory();

    long startTime = System.currentTimeMillis();
    List<StationResult> stationResults = null;
    try {
      stationResults = strategy.Analyze(filePath);
      result.success = true;
    } catch (Exception e) {
      result.error = e;
      result.success = false;
    }

    long executionTime = System.currentTimeMillis() - startTime;
    long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
    long memoryUsed = (memoryAfter - memoryBefore) / (1024 * 1024); // Convert to MB

    result.executionTimeMs = executionTime;
    result.memoryUsedMB = memoryUsed;
    result.resultCount = stationResults != null ? stationResults.size() : 0;

    // Validate results against expected output
    if (result.success && stationResults != null) {
      result.validation = validateResults(stationResults, filePath);
    }

    if (printResults && stationResults != null) {
      System.out.println(COLOR_CYAN + "Stations found by " + name + ":" + COLOR_RESET);
      stationResults.stream()
          .map(StationResult::getStationName)
          .sorted()
          .forEach(System.out::println);
      System.out.println();
    }

    return result;
  }

  private static ValidationResult validateResults(
      List<StationResult> actualResults, String dataFilePath) {
    ValidationResult validation = new ValidationResult();

    String expectedResultsFile = getExpectedResultsFile(dataFilePath);
    if (expectedResultsFile == null) {
      validation.isValid = false;
      validation.errors.add("Could not determine expected results file for: " + dataFilePath);
      return validation;
    }

    File file = new File(expectedResultsFile);
    if (!file.exists()) {
      validation.isValid = false;
      validation.errors.add("Expected results file not found: " + expectedResultsFile);
      return validation;
    }

    Map<String, ExpectedResult> expectedResults = loadExpectedResults(expectedResultsFile);
    if (expectedResults == null) {
      validation.isValid = false;
      validation.errors.add("Failed to load expected results from: " + expectedResultsFile);
      return validation;
    }

    Map<String, StationResult> actualMap = new HashMap<>();
    for (StationResult result : actualResults) {
      actualMap.put(result.getStationName(), result);
    }

    validation.totalStations = expectedResults.size();
    validation.isValid = true;

    for (Map.Entry<String, ExpectedResult> entry : expectedResults.entrySet()) {
      String stationName = entry.getKey();
      ExpectedResult expected = entry.getValue();

      if (!actualMap.containsKey(stationName)) {
        validation.missingStations++;
        validation.errors.add("Missing station: " + stationName);
        validation.isValid = false;
        continue;
      }

      StationResult actual = actualMap.get(stationName);
      if (!compareResults(stationName, expected, actual, validation)) {
        validation.mismatchedStations++;
        validation.isValid = false;
      } else {
        validation.matchedStations++;
      }
    }

    for (String stationName : actualMap.keySet()) {
      if (!expectedResults.containsKey(stationName)) {
        validation.extraStations++;
        validation.errors.add("Extra station not in expected results: " + stationName);
        validation.isValid = false;
      }
    }

    return validation;
  }

  private static String getExpectedResultsFile(String dataFilePath) {
    String fileName = new File(dataFilePath).getName();
    if (fileName.contains("100k") || fileName.contains("100000")) {
      return "../results/results-100k.csv";
    } else if (fileName.contains("1m") || fileName.contains("1000000")) {
      return "../results/results-1m.csv";
    } else if (fileName.contains("10m") || fileName.contains("10000000")) {
      return "../results/results-10m.csv";
    } else if (fileName.contains("100m") || fileName.contains("100000000")) {
      return "../results/results-100m.csv";
    }
    return null;
  }

  private static Map<String, ExpectedResult> loadExpectedResults(String csvFilePath) {
    Map<String, ExpectedResult> results = new HashMap<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
      String line = reader.readLine(); // Skip header
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(",");
        if (parts.length == 4) {
          String stationName = parts[0];
          double min = Double.parseDouble(parts[1]);
          double max = Double.parseDouble(parts[2]);
          double avg = Double.parseDouble(parts[3]);
          results.put(stationName, new ExpectedResult(min, max, avg));
        }
      }
    } catch (Exception e) {
      System.err.println("Error loading expected results: " + e.getMessage());
      return null;
    }
    return results;
  }

  private static boolean compareResults(
      String stationName,
      ExpectedResult expected,
      StationResult actual,
      ValidationResult validation) {
    // Convert actual values from long (scaled by 10) to double
    double actualMin = actual.getMin() / 10.0;
    double actualMax = actual.getMax() / 10.0;
    double actualAvg = actual.getCount() > 0 ? (double) actual.getSum() / (double) actual.getCount() / 10.0 : 0;

    // Allow small tolerance for floating point comparison (0.1 degrees)
    double tolerance = 0.1;

    boolean minMatches = Math.abs(actualMin - expected.min) <= tolerance;
    boolean maxMatches = Math.abs(actualMax - expected.max) <= tolerance;
    boolean avgMatches = Math.abs(actualAvg - expected.avg) <= tolerance;

    if (!minMatches || !maxMatches || !avgMatches) {
      validation.errors.add(
          String.format(
              "%s: Expected(min=%.1f, max=%.1f, avg=%.1f) vs Actual(min=%.1f, max=%.1f, avg=%.1f)",
              stationName,
              expected.min,
              expected.max,
              expected.avg,
              actualMin,
              actualMax,
              actualAvg));
      return false;
    }

    return true;
  }

  private static void printSummary(@NotNull List<BenchmarkResult> results) {
    System.out.println(
        COLOR_BOLD
            + COLOR_CYAN
            + "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            + COLOR_RESET);
    System.out.println(COLOR_BOLD + COLOR_CYAN + "  PERFORMANCE SUMMARY" + COLOR_RESET);
    System.out.println(
        COLOR_BOLD
            + COLOR_CYAN
            + "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            + COLOR_RESET);
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


    for (BenchmarkResult result : results) {
      if (result.success) {
        boolean isFastest = fastest != null && result.strategyName.equals(fastest.strategyName);
        String badge = isFastest ? COLOR_GREEN + " ★ FASTEST" + COLOR_RESET : "";

        String validationBadge = "";
        if (result.validation != null) {
          if (result.validation.isValid) {
            validationBadge = COLOR_GREEN + " ✓ VALID" + COLOR_RESET;
          } else {
            validationBadge = COLOR_RED + " ✗ INVALID" + COLOR_RESET;
          }
        }

        System.out.printf(
            "%s%-20s%s  %s%10s%s  │  %sMem: %3d MB%s  │  %sResults: %d%s%s%s%n",
            isFastest ? COLOR_GREEN : COLOR_RESET,
            result.strategyName,
            COLOR_RESET,
            COLOR_BOLD,
            formatDuration(result.executionTimeMs),
            COLOR_RESET,
            COLOR_BLUE,
            result.memoryUsedMB,
            COLOR_RESET,
            COLOR_YELLOW,
            result.resultCount,
            COLOR_RESET,
            badge,
            validationBadge);
      } else {
        System.out.printf(
            "%s%-20s%s  %s%10s%s  %s✗ FAILED: %s%s%n",
            COLOR_RED,
            result.strategyName,
            COLOR_RESET,
            COLOR_BOLD,
            "---",
            COLOR_RESET,
            COLOR_RED,
            result.error.getMessage(),
            COLOR_RESET);
      }
    }


    int successfulResults = 0;
    for (BenchmarkResult r : results) {
      if (r.success)
        successfulResults++;
    }

    if (successfulResults > 1 && fastest != null) {
      System.out.println();
      System.out.println(COLOR_BOLD + "Speedup vs " + fastest.strategyName + ":" + COLOR_RESET);
      for (BenchmarkResult result : results) {
        if (result.success && !result.strategyName.equals(fastest.strategyName)) {
          double ratio = (double) result.executionTimeMs / (double) fastest.executionTimeMs;
          String speedIndicator = ratio > 2.0 ? COLOR_RED : ratio > 1.5 ? COLOR_YELLOW : COLOR_GREEN;
          System.out.printf(
              "  %s%-20s%s  %s%.2fx slower%s%n",
              COLOR_RESET, result.strategyName, COLOR_RESET, speedIndicator, ratio, COLOR_RESET);
        }
      }
    }

    boolean hasInvalidResults = false;
    for (BenchmarkResult r : results) {
      if (r.success && r.validation != null && !r.validation.isValid) {
        hasInvalidResults = true;
        break;
      }
    }

    if (hasInvalidResults) {
      System.out.println();
      System.out.println(COLOR_BOLD + COLOR_RED + "Validation Errors:" + COLOR_RESET);
      for (BenchmarkResult result : results) {
        if (result.success && result.validation != null && !result.validation.isValid) {
          System.out.println();
          System.out.printf(
              "%s%s:%s%n", COLOR_YELLOW + COLOR_BOLD, result.strategyName, COLOR_RESET);
          System.out.printf(
              "  Total: %d  Matched: %d  Mismatched: %d  Missing: %d  Extra: %d%n",
              result.validation.totalStations,
              result.validation.matchedStations,
              result.validation.mismatchedStations,
              result.validation.missingStations,
              result.validation.extraStations);

          if (!result.validation.errors.isEmpty()) {
            int errorsToShow = Math.min(10, result.validation.errors.size());
            System.out.printf("  Showing first %d errors:%n", errorsToShow);
            for (int i = 0; i < errorsToShow; i++) {
              System.out.println("    " + result.validation.errors.get(i));
            }
            if (result.validation.errors.size() > errorsToShow) {
              System.out.printf(
                  "    ... and %d more errors%n", result.validation.errors.size() - errorsToShow);
            }
          }
        }
      }
    }

    System.out.println();
    System.out.println(
        COLOR_BOLD
            + COLOR_CYAN
            + "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            + COLOR_RESET);
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

  static class BenchmarkResult {
    String strategyName;
    long executionTimeMs;
    long memoryUsedMB;
    int resultCount;
    boolean success;
    Exception error;
    ValidationResult validation;

    BenchmarkResult(String strategyName) {
      this.strategyName = strategyName;
      this.success = false;
    }
  }

  static class ValidationResult {
    boolean isValid;
    int totalStations;
    int matchedStations;
    int mismatchedStations;
    int missingStations;
    int extraStations;
    List<String> errors;

    ValidationResult() {
      this.errors = new ArrayList<>();
    }
  }

  static class ExpectedResult {
    double min;
    double max;
    double avg;

    ExpectedResult(double min, double max, double avg) {
      this.min = min;
      this.max = max;
      this.avg = avg;
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