package com.onebillion.benchmarks;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Example JMH Benchmark demonstrating various benchmarking patterns
 *
 * <p>This shows: - Different benchmark modes - State management - Parameter testing -
 * Setup/TearDown methods
 */
@State(Scope.Thread)
@Fork(1)
public class ExampleBenchmark {

  // ═══════════════════════════════════════════════════════════════════
  // Throughput Benchmark - How many operations per second
  // ═══════════════════════════════════════════════════════════════════
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 2, time = 1)
  @Measurement(iterations = 3, time = 1)
  public void throughputExample(Blackhole blackhole) {
    List<Integer> list = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      list.add(i);
    }
    blackhole.consume(list);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Average Time Benchmark - Average time per operation
  // ═══════════════════════════════════════════════════════════════════
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 2, time = 1)
  @Measurement(iterations = 3, time = 1)
  public void averageTimeExample(Blackhole blackhole) {
    Map<String, Integer> map = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      map.put("key" + i, i);
    }
    blackhole.consume(map);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Parameterized Benchmark - Test with different inputs
  // ═══════════════════════════════════════════════════════════════════
  @State(Scope.Benchmark)
  public static class ParamState {
    @Param({"100", "1000", "10000"})
    public int size;

    public List<String> data;

    @Setup(Level.Trial)
    public void setup() {
      data = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        data.add("item" + i);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 1)
  @Measurement(iterations = 2)
  public void parameterizedExample(ParamState state, Blackhole blackhole) {
    List<String> filtered =
        state.data.stream().filter(s -> s.startsWith("item")).collect(Collectors.toList());
    blackhole.consume(filtered);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Comparing Multiple Approaches
  // ═══════════════════════════════════════════════════════════════════
  @State(Scope.Thread)
  public static class MapComparisonState {
    private Map<String, Integer> hashMap;
    private Map<String, Integer> concurrentHashMap;
    private Map<String, Integer> treeMap;

    @Setup(Level.Invocation)
    public void setup() {
      hashMap = new HashMap<>();
      concurrentHashMap = new ConcurrentHashMap<>();
      treeMap = new TreeMap<>();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void hashMapPut(MapComparisonState state, Blackhole blackhole) {
    state.hashMap.put("test-key", 42);
    blackhole.consume(state.hashMap);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void concurrentHashMapPut(MapComparisonState state, Blackhole blackhole) {
    state.concurrentHashMap.put("test-key", 42);
    blackhole.consume(state.concurrentHashMap);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void treeMapPut(MapComparisonState state, Blackhole blackhole) {
    state.treeMap.put("test-key", 42);
    blackhole.consume(state.treeMap);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Sample Time Benchmark - Statistical distribution of timings
  // ═══════════════════════════════════════════════════════════════════
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 1)
  @Measurement(iterations = 2)
  public void sampleTimeExample(Blackhole blackhole) {
    List<Double> numbers = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      numbers.add(Math.random() * 100);
    }
    Collections.sort(numbers);
    blackhole.consume(numbers);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Main method to run benchmarks
  // ═══════════════════════════════════════════════════════════════════
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(ExampleBenchmark.class.getSimpleName()).build();

    new Runner(opt).run();
  }
}
