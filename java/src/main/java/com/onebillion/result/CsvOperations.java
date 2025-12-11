package com.onebillion.result;

import com.onebillion.strategies.StationResult;
import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class CsvOperations {
  public static Map<String, ParsedResult> loadExpectedResults(String resultsFilePath)
      throws IOException {
    Map<String, ParsedResult> results = new HashMap<>();

    try (var reader = new BufferedReader(new FileReader(resultsFilePath))) {
      var line = reader.readLine(); // Skip header
      while ((line = reader.readLine()) != null) {
        String[] columns = line.split(",");

        if (columns.length != 4) {
          System.out.println("Warning: Skipping malformed line: " + line);
        }

        if (columns.length < 4) {
          continue;
        }
        var stationName = columns[0];
        var min = Double.parseDouble(columns[1]);
        var avg = Double.parseDouble(columns[2]);
        var max = Double.parseDouble(columns[3]);
        results.put(stationName, new ParsedResult(min, max, avg));
      }
    }
    return results;
  }

  public static void saveResultsToCSV(String outputFilePath, @NotNull List<StationResult> results)
      throws IOException {
    try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
      writer.println("StationName,Min,Max,Avg");
      results.stream()
          .sorted(Comparator.comparing(StationResult::getStationName))
          .forEach(
              station -> {
                double min = station.getMin() / 10.0;
                double max = station.getMax() / 10.0;
                double avg =
                    station.getCount() > 0
                        ? (double) station.getSum() / (double) station.getCount() / 10.0
                        : 0.0;
                writer.printf("%s,%.1f,%.1f,%.1f%n", station.getStationName(), min, max, avg);
              });
    }
  }
}
