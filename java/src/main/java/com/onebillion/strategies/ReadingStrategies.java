package com.onebillion.strategies;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Collection of Strategy implementations that combine different ChunkReader and LineReader
 * implementations.
 *
 * <p>This file contains 8 strategies representing all combinations of:
 *
 * <ul>
 *   <li>4 ChunkReader implementations: StandardBuffered, MemoryMapped, ByteBuffered, Arena
 *   <li>2 LineReader implementations: HashTable, LinearProbing
 * </ul>
 *
 * <p>Each strategy leverages the ChunkReadStrategy framework for parallel file processing.
 */
public class ReadingStrategies {

  private static final int TABLE_SIZE = 131072; // 2^17, matches existing strategies

  // ========== StandardBufferedReader Combinations ==========

  public static class StandardBufferedHashTableStrategy implements Strategy {
    @Override
    public List<StationResult> Analyze(String filepath) throws IOException {
      var chunkStrategy = new ChunkReadStrategy(filepath);
      var chunkReader = new ChunkReadStrategy.StandardBufferedReader();
      var lineReader = new HashTable();
      try {
        return chunkStrategy.runPlan(chunkReader, lineReader);
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException("Error processing chunks: " + e.getMessage(), e);
      }
    }
  }

  public static class StandardBufferedLinearProbingStrategy implements Strategy {
    @Override
    public List<StationResult> Analyze(String filepath) throws IOException {
      var chunkStrategy = new ChunkReadStrategy(filepath);
      var chunkReader = new ChunkReadStrategy.StandardBufferedReader();
      var lineReader = new LinearProbing(TABLE_SIZE);
      try {
        return chunkStrategy.runPlan(chunkReader, lineReader);
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException("Error processing chunks: " + e.getMessage(), e);
      }
    }
  }

  // ========== MemoryMappedBufferedReader Combinations ==========

  public static class MemoryMappedHashTableStrategy implements Strategy {
    @Override
    public List<StationResult> Analyze(String filepath) throws IOException {
      var chunkStrategy = new ChunkReadStrategy(filepath);
      var chunkReader = new ChunkReadStrategy.MemoryMappedBufferedReader();
      var lineReader = new HashTable();
      try {
        return chunkStrategy.runPlan(chunkReader, lineReader);
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException("Error processing chunks: " + e.getMessage(), e);
      }
    }
  }

  public static class MemoryMappedLinearProbingStrategy implements Strategy {
    @Override
    public List<StationResult> Analyze(String filepath) throws IOException {
      var chunkStrategy = new ChunkReadStrategy(filepath);
      var chunkReader = new ChunkReadStrategy.MemoryMappedBufferedReader();
      var lineReader = new LinearProbing(TABLE_SIZE);
      try {
        return chunkStrategy.runPlan(chunkReader, lineReader);
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException("Error processing chunks: " + e.getMessage(), e);
      }
    }
  }

  // ========== ByteBufferedReader Combinations ==========

  public static class ByteBufferedHashTableStrategy implements Strategy {
    @Override
    public List<StationResult> Analyze(String filepath) throws IOException {
      var chunkStrategy = new ChunkReadStrategy(filepath);
      var chunkReader = new ChunkReadStrategy.ByteBufferedReader();
      var lineReader = new HashTable();
      try {
        return chunkStrategy.runPlan(chunkReader, lineReader);
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException("Error processing chunks: " + e.getMessage(), e);
      }
    }
  }

  public static class ByteBufferedLinearProbingStrategy implements Strategy {
    @Override
    public List<StationResult> Analyze(String filepath) throws IOException {
      var chunkStrategy = new ChunkReadStrategy(filepath);
      var chunkReader = new ChunkReadStrategy.ByteBufferedReader();
      var lineReader = new LinearProbing(TABLE_SIZE);
      try {
        return chunkStrategy.runPlan(chunkReader, lineReader);
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException("Error processing chunks: " + e.getMessage(), e);
      }
    }
  }

  // ========== ArenaReader Combinations ==========

  public static class ArenaHashTableStrategy implements Strategy {
    @Override
    public List<StationResult> Analyze(String filepath) throws IOException {
      var chunkStrategy = new ChunkReadStrategy(filepath);
      var chunkReader = new ChunkReadStrategy.ArenaReader();
      var lineReader = new HashTable();
      try {
        return chunkStrategy.runPlan(chunkReader, lineReader);
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException("Error processing chunks: " + e.getMessage(), e);
      }
    }
  }

  public static class ArenaLinearProbingStrategy implements Strategy {
    @Override
    public List<StationResult> Analyze(String filepath) throws IOException {
      var chunkStrategy = new ChunkReadStrategy(filepath);
      var chunkReader = new ChunkReadStrategy.ArenaReader();
      var lineReader = new LinearProbing(TABLE_SIZE);
      try {
        return chunkStrategy.runPlan(chunkReader, lineReader);
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException("Error processing chunks: " + e.getMessage(), e);
      }
    }
  }
}
