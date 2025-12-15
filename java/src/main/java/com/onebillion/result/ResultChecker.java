package com.onebillion.result;

import com.onebillion.strategies.StationResult;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ResultChecker {

  private static final double TOLERANCE = 0.2;

  public static ValidationResult validateResults(
      List<StationResult> actualResults, String dataFilePath) {
    var validation = new ValidationResult();

    String expectedResultsFile = getExpectedResultsFile(dataFilePath);
    if (expectedResultsFile == null) {
      return validation.addError("Could not determine expected results file for: " + dataFilePath);
    }

    var file = new File(expectedResultsFile);
    if (!file.exists()) {
      return validation.addError("Expected results file not found: " + expectedResultsFile);
    }

    Map<String, ParsedResult> expectedResults;
    try {
      expectedResults = CsvOperations.loadExpectedResults(expectedResultsFile);
    } catch (IOException e) {
      return validation.addError("Error reading expected results file: " + e.getMessage());
    }

    Map<String, StationResult> actualMap =
        actualResults.stream()
            .collect(Collectors.toMap(StationResult::getStationName, Function.identity()));

    validation.totalStations = expectedResults.size();
    validation.isValid = true;

    expectedResults.forEach(
        (stationName, expected) -> {
          if (!actualMap.containsKey(stationName)) {
            validation.missingStations++;
            validation.addError("Missing station: " + stationName);
            return;
          }

          var actual = actualMap.get(stationName);
          if (!compareResults(stationName, expected, actual, validation)) {
            validation.mismatchedStations++;
            validation.isValid = false;
          } else {
            validation.matchedStations++;
          }
        });

    actualMap.keySet().stream()
        .filter(station -> !expectedResults.containsKey(station))
        .forEach(
            stationName -> {
              validation.extraStations++;
              validation.addError("Extra station not in expected results: " + stationName);
            });

    return validation;
  }

  public static boolean compareResults(
      String stationName,
      @NotNull ParsedResult expected,
      @NotNull StationResult actual,
      ValidationResult validation) {
    var actualMin = actual.getMin() / 10.0;
    var actualMax = actual.getMax() / 10.0;
    var actualAvg =
        actual.getCount() > 0 ? (double) actual.getSum() / (double) actual.getCount() / 10.0 : 0;

    boolean minMatches = Math.abs(actualMin - expected.min()) <= TOLERANCE;
    boolean maxMatches = Math.abs(actualMax - expected.max()) <= TOLERANCE;
    boolean avgMatches = Math.abs(actualAvg - expected.avg()) <= TOLERANCE;

    if (!minMatches || !maxMatches || !avgMatches) {
      var errs = new StringBuilder(stationName).append(": ");

      if (!minMatches) {
        errs.append(String.format("min(expected=%.1f, actual=%.1f)", expected.min(), actualMin));
      }
      if (!maxMatches) {
        if (!minMatches) errs.append(", ");
        errs.append(String.format("max(expected=%.1f, actual=%.1f)", expected.max(), actualMax));
      }
      if (!avgMatches) {
        if (!minMatches || !maxMatches) errs.append(", ");
        errs.append(String.format("avg(expected=%.1f, actual=%.1f)", expected.avg(), actualAvg));
      }

      validation.errors.add(errs.toString());
      return false;
    }

    return true;
  }

  private static @Nullable String getExpectedResultsFile(String dataFilePath) {
    var fileName = new File(dataFilePath).getName();
    if (fileName.contains("100k") || fileName.contains("100000")) {
      return "../results/results-100k.csv";
    }

    if (fileName.contains("1m") || fileName.contains("1000000")) {
      return "../results/results-1m.csv";
    }

    if (fileName.contains("10m") || fileName.contains("10000000")) {
      return "../results/results-10m.csv";
    }

    if (fileName.contains("100m") || fileName.contains("100000000")) {
      return "../results/results-100m.csv";
    }

    if (fileName.contains("1b") || fileName.contains("1000000000")) {
      return "../results/results-1b.csv";
    }

    return null;
  }
}
