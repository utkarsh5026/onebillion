package com.onebillion.benchmarks;

import com.onebillion.strategies.LineParser;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH Benchmark for LineParser operations
 *
 * <p>This benchmark demonstrates micro-benchmarking of individual methods
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class LineParserBenchmark {

  @Param({"Station1;10.5", "VeryLongStationNameHere;-25.75", "Short;0.0"})
  private String line;

  @Benchmark
  public void benchmarkParseLine(Blackhole blackhole) {
    var station = LineParser.parseLineStandard(line);
    blackhole.consume(station);
  }

  @Benchmark
  public void benchmarkParseLineWithValidation(Blackhole blackhole) {
    if (line != null && !line.isEmpty()) {
      var station = LineParser.parseLineStandard(line);
      blackhole.consume(station);
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(LineParserBenchmark.class.getSimpleName()).build();

    new Runner(opt).run();
  }
}
