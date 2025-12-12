package com.onebillion.result;

import static com.onebillion.result.Color.*;

import com.onebillion.strategies.StationResult;
import com.onebillion.strategies.Strategy;
import java.io.IOException;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class StrategyRunner {
  public static BenchmarkResult benchmarkStrategy(
      String name, Strategy strategy, String filePath, boolean printResults) {

    BenchmarkResult result = new BenchmarkResult(name);
    System.gc();

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    var results = run(strategy, filePath, result);

    if (result.success && results != null) {
      saveResultsToCSV(name, results);
    }

    if (result.success && results != null) {
      result.validation = ResultChecker.validateResults(results, filePath);
    }

    if (printResults && results != null) {
      System.out.println(COLOR_CYAN + "Stations found by " + name + ":" + COLOR_RESET);
      results.stream().map(StationResult::getStationName).sorted().forEach(System.out::println);
      System.out.println();
    }

    return result;
  }

  private static List<StationResult> run(
      Strategy strategy, String filePath, BenchmarkResult result) {
    var runtime = Runtime.getRuntime();
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
    long memoryUsed = (memoryAfter - memoryBefore) / (1024 * 1024);

    result.executionTimeMs = executionTime;
    result.memoryUsedMB = memoryUsed;
    result.resultCount = stationResults != null ? stationResults.size() : 0;
    return stationResults;
  }

  private static void saveResultsToCSV(@NotNull String strategyName, List<StationResult> results) {
    var fileName = strategyName.replaceAll("[^a-zA-Z0-9-]", "_").toLowerCase();
    var filePath = "results/" + fileName + ".csv";
    try {
      CsvOperations.saveResultsToCSV(filePath, results);
      System.out.println(COLOR_GREEN + "  ✓ Results saved to: " + filePath + COLOR_RESET);
    } catch (IOException e) {
      System.err.println(
          COLOR_RED + "  ✗ Failed to save results to CSV: " + e.getMessage() + COLOR_RESET);
    }
  }
}
