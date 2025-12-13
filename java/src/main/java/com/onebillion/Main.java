package com.onebillion;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import com.onebillion.result.BenchmarkResult;
import static com.onebillion.result.Color.COLOR_BLUE;
import static com.onebillion.result.Color.COLOR_BOLD;
import static com.onebillion.result.Color.COLOR_CYAN;
import static com.onebillion.result.Color.COLOR_RESET;
import static com.onebillion.result.Color.COLOR_YELLOW;
import com.onebillion.result.StrategyRunner;
import com.onebillion.strategies.ChunkReadStrategy;
import com.onebillion.strategies.ChunkReadStrategy.ArenaReader;
import com.onebillion.strategies.ChunkReadStrategy.ByteBufferedReader;
import com.onebillion.strategies.ChunkReadStrategy.ChunkReader;
import com.onebillion.strategies.ChunkReadStrategy.MemoryMappedBufferedReader;
import com.onebillion.strategies.ChunkReadStrategy.StandardBufferedReader;
import com.onebillion.strategies.HashTable;
import com.onebillion.strategies.LineReader;
import com.onebillion.strategies.LinearProbing;
import com.onebillion.strategies.Strategy;

public class Main {

  private static final int TABLE_SIZE = 131072; // 2^17

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
    String[] fileArgs = printResults && args.length > 1
        ? new String[] { args[1] }
        : printResults ? new String[0] : args;
    String dataFile = getDataFile(fileArgs);

    // Define ChunkReaders array
    ChunkReaderConfig[] chunkReaders = new ChunkReaderConfig[] {
        new ChunkReaderConfig("StandardBuffered", new StandardBufferedReader()),
        new ChunkReaderConfig("MemoryMapped", new MemoryMappedBufferedReader()),
        new ChunkReaderConfig("ByteBuffered", new ByteBufferedReader()),
        new ChunkReaderConfig("Arena", new ArenaReader())
    };

    // Define LineReaders array
    LineReaderConfig[] lineReaders = new LineReaderConfig[] {
        new LineReaderConfig("HashTable", HashTable::new),
        new LineReaderConfig("LinearProbing", () -> new LinearProbing(TABLE_SIZE))
    };

    List<StrategyWrapper> strategies = new ArrayList<>();
    for (ChunkReaderConfig chunkReader : chunkReaders) {
      for (LineReaderConfig lineReader : lineReaders) {
        String name = chunkReader.name + "-" + lineReader.name;
        Strategy strategy = createStrategy(chunkReader.chunkReader, lineReader.lineReaderSupplier);
        strategies.add(new StrategyWrapper(name, strategy));
      }
    }

    List<BenchmarkResult> results = new ArrayList<>();

    for (StrategyWrapper s : strategies) {
      System.out.printf("%s⏱️  Running: %s%s%n", COLOR_YELLOW, s.name, COLOR_RESET);
      var result = StrategyRunner.benchmarkStrategy(s.name, s.strategy, dataFile, printResults);
      results.add(result);
      result.printExecutionResult();
    }

    BenchmarkResult.printSummary(results);
  }

  private static Strategy createStrategy(
      ChunkReader chunkReader, Supplier<LineReader> lineReaderSupplier) {
    return filepath -> {
      var chunkStrategy = new ChunkReadStrategy(filepath);
      try {
        return chunkStrategy.runPlan(chunkReader, lineReaderSupplier);
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException("Error processing chunks: " + e.getMessage(), e);
      }
    };
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

  static class ChunkReaderConfig {
    String name;
    ChunkReader chunkReader;

    ChunkReaderConfig(String name, ChunkReader chunkReader) {
      this.name = name;
      this.chunkReader = chunkReader;
    }
  }

  static class LineReaderConfig {
    String name;
    Supplier<LineReader> lineReaderSupplier;

    LineReaderConfig(String name, Supplier<LineReader> lineReaderSupplier) {
      this.name = name;
      this.lineReaderSupplier = lineReaderSupplier;
    }
  }
}
