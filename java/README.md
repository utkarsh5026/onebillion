# One Billion Row Challenge - Java Implementation

Java implementation of the One Billion Row Challenge benchmark.

## Project Structure

```
java/
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/
│   │           └── onebillion/
│   │               ├── Main.java           # Main entry point
│   │               └── strategies/         # Strategy implementations
├── target/                                 # Compiled classes (generated)
├── Makefile                                # Build automation
├── pom.xml                                 # Maven configuration
├── run.sh                                  # Unix/Linux/Mac run script
└── run.bat                                 # Windows run script
```

## Prerequisites

- **Java 21** or higher
- **Maven** (optional, for Maven-based builds)
- **Make** (for Makefile commands)
- **Python** (for data generation)

## Quick Start

### Using Makefile (Recommended)

```bash
# View all available commands
make help

# Build the project
make build

# Run with default data
make run

# Quick benchmark (1K rows)
make benchmark
```

### Using Run Scripts

**Windows:**
```cmd
run.bat
```

**Unix/Linux/Mac:**
```bash
./run.sh
```

### Using Maven

```bash
# Compile
mvn compile

# Run
mvn exec:java

# Create JAR
mvn package

# Run JAR
java -jar target/onebillion-challenge.jar
```

## Available Make Commands

### Build & Run
- `make build` - Compile all Java source files
- `make run` - Run benchmark with default data
- `make jar` - Create executable JAR file
- `make test` - Run JUnit tests

### Benchmarks
- `make benchmark` - Quick benchmark (1K rows, instant)
- `make bench-small` - Small benchmark (10M rows, ~30s)
- `make bench-medium` - Medium benchmark (100M rows, ~5min)
- `make bench-large` - Large benchmark (500M rows, ~20min)
- `make bench-billion` - Full 1B benchmark (1B rows, ~45min!)

### Code Quality
- `make format` - Format code (requires google-java-format)
- `make check` - Run code quality checks

### Cleanup
- `make clean` - Remove compiled files and generated data
- `make clean-all` - Remove everything (target, cache, data)

## Adding Strategy Implementations

1. Create a new strategy class in [src/main/java/com/onebillion/strategies/](src/main/java/com/onebillion/strategies/)
2. Implement your strategy interface
3. Add it to the benchmark runner in [Main.java](src/main/java/com/onebillion/Main.java)
4. Run benchmarks to compare performance

## Example Strategy Structure

```java
package com.onebillion.strategies;

public interface Strategy {
    List<StationResult> calculate(String filePath) throws Exception;
}

public class BasicStrategy implements Strategy {
    @Override
    public List<StationResult> calculate(String filePath) throws Exception {
        // Your implementation here
    }
}
```

## Performance Tips

1. Use memory-mapped files for large datasets
2. Leverage parallel streams and concurrent processing
3. Optimize string parsing and avoid unnecessary allocations
4. Use primitive types instead of boxed types where possible
5. Profile with JFR (Java Flight Recorder) to identify bottlenecks

## Building from Scratch

```bash
# Clone and navigate
cd java

# Build
make build

# Run quick test
make benchmark

# Run full benchmark
make bench-billion
```

## Notes

- The project uses Java 21 features for optimal performance
- Compiled classes are output to `target/classes/`
- Data files are stored in `../data/`
- The runner auto-detects the most recent measurements file

## Troubleshooting

**"Command not found: make"**
- Windows: Install Make via Chocolatey (`choco install make`) or use Maven
- Mac: Install via Homebrew (`brew install make`)
- Linux: Install via package manager (`apt-get install make` or `yum install make`)

**Java version issues**
- Ensure Java 21+ is installed: `java -version`
- Update `maven.compiler.source` and `maven.compiler.target` in pom.xml if needed

**Memory issues with large datasets**
- Increase heap size: `java -Xmx8g -jar target/onebillion-challenge.jar`
- Use the Makefile which handles this automatically
