package com.onebillion.result;

import static com.onebillion.result.Color.*;

import java.util.Comparator;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class BenchmarkResult {
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

  private static @NotNull String formatDuration(long milliseconds) {
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

  public static void printSummary(List<BenchmarkResult> results) {
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
    }

    BenchmarkResult fastest =
        results.stream()
            .filter(r -> r.success)
            .min(Comparator.comparingLong(r -> r.executionTimeMs))
            .orElse(null);

    printResultsComparison(results, fastest);

    long successfulResults = results.stream().filter(r -> r.success).count();

    if (successfulResults > 1 && fastest != null) {
      printSuccessStats(results, fastest);
    }

    var hasInvalidResults =
        results.stream().anyMatch(r -> r.success && r.validation != null && !r.validation.isValid);

    if (hasInvalidResults) {
      printInvalidResults(results);
    }

    System.out.println();
    System.out.println(
        COLOR_BOLD
            + COLOR_CYAN
            + "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            + COLOR_RESET);
  }

  private static void printResultsComparison(
      @NotNull List<BenchmarkResult> results, BenchmarkResult fastest) {
    for (var r : results) {
      if (!r.success) {
        System.out.printf(
            "%s%-20s%s  %s%10s%s  %s✗ FAILED: %s%s%n",
            COLOR_RED,
            r.strategyName,
            COLOR_RESET,
            COLOR_BOLD,
            "---",
            COLOR_RESET,
            COLOR_RED,
            r.error.getMessage(),
            COLOR_RESET);
        continue;
      }

      var isFastest = fastest != null && r.strategyName.equals(fastest.strategyName);
      String badge = isFastest ? COLOR_GREEN + " ★ FASTEST" + COLOR_RESET : "";

      String validationBadge = "";
      if (r.validation != null) {
        if (r.validation.isValid) {
          validationBadge = COLOR_GREEN + " ✓ VALID" + COLOR_RESET;
        } else {
          validationBadge = COLOR_RED + " ✗ INVALID" + COLOR_RESET;
        }
      }

      System.out.printf(
          "%s%-20s%s  %s%10s%s  │  %sMem: %3d MB%s  │  %sResults: %d%s%s%s%n",
          isFastest ? COLOR_GREEN : COLOR_RESET,
          r.strategyName,
          COLOR_RESET,
          COLOR_BOLD,
          formatDuration(r.executionTimeMs),
          COLOR_RESET,
          COLOR_BLUE,
          r.memoryUsedMB,
          COLOR_RESET,
          COLOR_YELLOW,
          r.resultCount,
          COLOR_RESET,
          badge,
          validationBadge);
    }
  }

  private static void printSuccessStats(
      @NotNull List<BenchmarkResult> results, @NotNull BenchmarkResult fastest) {
    System.out.println();
    System.out.println(COLOR_BOLD + "Speedup vs " + fastest.strategyName + ":" + COLOR_RESET);
    for (BenchmarkResult r : results) {
      if (r.success && !r.strategyName.equals(fastest.strategyName)) {
        var ratio = (double) r.executionTimeMs / (double) fastest.executionTimeMs;
        var speedIndicator = ratio > 2.0 ? COLOR_RED : ratio > 1.5 ? COLOR_YELLOW : COLOR_GREEN;
        System.out.printf(
            "  %s%-20s%s  %s%.2fx slower%s%n",
            COLOR_RESET, r.strategyName, COLOR_RESET, speedIndicator, ratio, COLOR_RESET);
      }
    }
  }

  private static void printInvalidResults(@NotNull List<BenchmarkResult> results) {
    System.out.println();
    System.out.println(COLOR_BOLD + COLOR_RED + "Validation Errors:" + COLOR_RESET);
    for (BenchmarkResult result : results) {
      if (result.success && result.validation != null && !result.validation.isValid) {
        System.out.println();
        System.out.printf("%s%s:%s%n", COLOR_YELLOW + COLOR_BOLD, result.strategyName, COLOR_RESET);
        System.out.printf(
            "  Total: %d  Matched: %d  Mismatched: %d  Missing: %d  Extra: %d%n",
            result.validation.totalStations,
            result.validation.matchedStations,
            result.validation.mismatchedStations,
            result.validation.missingStations,
            result.validation.extraStations);

        if (!result.validation.errors.isEmpty()) {
          var errorsToShow = Math.min(20, result.validation.errors.size());
          result.validation.errors.stream()
              .limit(errorsToShow)
              .forEach(err -> System.out.println("    " + err));

          if (result.validation.errors.size() > errorsToShow) {
            System.out.printf(
                "    ... and %d more errors%n", result.validation.errors.size() - errorsToShow);
          }
        }
      }
    }
  }

  public void printExecutionResult() {
    if (success) {
      System.out.printf(
          "%s✓ Completed in: %s%s%n%n",
          Color.COLOR_GREEN, formatDuration(executionTimeMs), COLOR_RESET);
    } else {
      System.out.printf("%s✗ Failed: %s%s%n%n", Color.COLOR_RED, error.getMessage(), COLOR_RESET);
    }
  }
}
