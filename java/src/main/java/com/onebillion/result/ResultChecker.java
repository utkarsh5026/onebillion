package com.onebillion.result;

import com.onebillion.strategies.StationResult;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class ResultChecker {

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

    Map<String, ParsedResult> expectedResults = new HashMap<>();
    try {
      expectedResults = CsvOperations.loadExpectedResults(expectedResultsFile);
    } catch (IOException e) {
      return validation.addError("Error reading expected results file: " + e.getMessage());
    }

    Map<String, StationResult> actualMap = new HashMap<>();
    for (StationResult result : actualResults) {
      actualMap.put(result.getStationName(), result);
    }

    validation.totalStations = expectedResults.size();
    validation.isValid = true;

    for (Map.Entry<String, ParsedResult> entry : expectedResults.entrySet()) {
      String stationName = entry.getKey();
      ParsedResult expected = entry.getValue();

      if (!actualMap.containsKey(stationName)) {
        validation.missingStations++;
        validation.addError("Missing station: " + stationName);
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
        validation.addError("Extra station not in expected results: " + stationName);
      }
    }

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

    double tolerance = 0.1;

    boolean minMatches = Math.abs(actualMin - expected.min()) <= tolerance;
    boolean maxMatches = Math.abs(actualMax - expected.max()) <= tolerance;
    boolean avgMatches = Math.abs(actualAvg - expected.avg()) <= tolerance;

    if (!minMatches || !maxMatches || !avgMatches) {
      validation.errors.add(
          String.format(
              "%s: Expected(min=%.1f, max=%.1f, avg=%.1f) vs Actual(min=%.1f, max=%.1f, avg=%.1f)",
              stationName,
              expected.min(),
              expected.max(),
              expected.avg(),
              actualMin,
              actualMax,
              actualAvg));
      return false;
    }

    return true;
  }

  private static String getExpectedResultsFile(String dataFilePath) {
    var fileName = new File(dataFilePath).getName();
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
}
