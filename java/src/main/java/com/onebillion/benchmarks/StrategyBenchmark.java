package com.onebillion.benchmarks;

import com.onebillion.strategies.BatchStrategy;
import com.onebillion.strategies.MCMPStrategy;
import com.onebillion.strategies.Strategy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH Benchmark for different processing strategies
 *
 * <p>Usage: mvn clean install java -jar target/benchmarks.jar StrategyBenchmark
 *
 * <p>Or use make: make benchmark-jmh
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgs = {"-Xms2G", "-Xmx2G"})
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
public class StrategyBenchmark {

  @Param({"../data/measurements-1k.txt", "../data/measurements-10m.txt"})
  private String inputFile;

  private Path filePath;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    filePath = Paths.get(inputFile);

    if (!Files.exists(filePath)) {
      System.err.println("Warning: File " + inputFile + " does not exist. Creating dummy file...");
      Files.createDirectories(filePath.getParent());
      Files.writeString(filePath, "Station1;10.5\nStation2;-5.2\nStation3;25.0\n");
    }
  }

  @Benchmark
  public void benchmarkBatchStrategy(Blackhole blackhole) throws Exception {
    Strategy strategy = new BatchStrategy();
    var result = strategy.Analyze(filePath.toString());
    blackhole.consume(result);
  }

  @Benchmark
  public void benchmarkMCMPStrategy(Blackhole blackhole) throws Exception {
    Strategy strategy = new MCMPStrategy();
    var result = strategy.Analyze(filePath.toString());
    blackhole.consume(result);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(StrategyBenchmark.class.getSimpleName()).build();

    new Runner(opt).run();
  }
}
