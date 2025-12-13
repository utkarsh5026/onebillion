<div align="center">
  <img src="logo.png" alt="One Billion Row Challenge" width="400"/>

  # One Billion Row Challenge

  **A high-performance data processing benchmark - Processing 1 billion temperature measurements**

  [![Java CI](https://github.com/utkarsh5026/onebillion/workflows/Java%20CI/badge.svg)](https://github.com/utkarsh5026/onebillion/actions)
  [![Go CI](https://github.com/utkarsh5026/onebillion/workflows/Go%20CI/badge.svg)](https://github.com/utkarsh5026/onebillion/actions)
  [![Python CI](https://github.com/utkarsh5026/onebillion/workflows/Python%20CI/badge.svg)](https://github.com/utkarsh5026/onebillion/actions)

  [Features](#-features) • [Quick Start](#-quick-start) • [Implementations](#-implementations) • [Benchmarks](#-benchmarks) • [Documentation](#-documentation)
</div>

---

## 📖 About

The **One Billion Row Challenge** is a performance optimization challenge that tests your ability to efficiently process and aggregate one billion temperature measurements from weather stations. This repository contains optimized implementations in **Java** and **Go**, showcasing various strategies and techniques for handling massive datasets.

### The Challenge

Process a text file containing 1,000,000,000 rows of temperature measurements and calculate min, max, and average temperatures for each weather station as fast as possible.

**Input Format:**
```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
...
```

**Expected Output:**
```
{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, ...}
```

---

## ✨ Features

- **🚀 Multi-Language Support** - Java 21 and Go implementations
- **⚡ High Performance** - Memory-mapped files, parallel processing, and optimized data structures
- **📊 Multiple Strategies** - Compare different approaches and algorithms
- **🔧 Easy Data Generation** - Built-in tools to generate test datasets of various sizes
- **✅ Verification Tools** - Validate results against expected outputs
- **📈 Benchmarking Suite** - Comprehensive performance testing
- **🎯 CI/CD Integration** - Automated testing with GitHub Actions

---

## 🏗️ Project Structure

```
onebillion/
├── java/                      # Java 21 implementation
│   ├── src/main/java/com/onebillion/
│   │   ├── Main.java         # Main benchmark runner
│   │   └── strategies/       # Strategy implementations
│   ├── Makefile              # Java-specific build commands
│   └── README.md             # Java documentation
├── golang/                    # Go implementation
│   ├── main.go               # Main benchmark runner
│   ├── strategies/           # Strategy implementations
│   ├── Makefile              # Go-specific build commands
│   └── README.md             # Go documentation
├── data/                      # Generated datasets (ignored in git)
├── results/                   # Benchmark results
├── verify/                    # Verification outputs
├── generate.py               # Data generation script
├── verify.py                 # Result verification script
├── Makefile                  # Root-level commands
└── README.md                 # This file
```

---

## 🚀 Quick Start

### Prerequisites

- **Java 21+** (for Java implementation)
- **Go 1.21+** (for Go implementation)
- **Python 3.8+** (for data generation and verification)
- **Make** (optional, but recommended)

### Installation

```bash
# Clone the repository
git clone https://github.com/utkarsh5026/onebillion.git
cd onebillion

# Install Python dependencies
pip install -r requirements.txt
```

### Generate Test Data

```bash
# Quick test with 1M rows (~14MB, takes seconds)
make generate-tiny

# Standard test with 10M rows (~140MB, takes ~10s)
make generate-small

# Large test with 100M rows (~1.4GB, takes ~2min)
make generate-large

# Full challenge with 1B rows (~14GB, takes ~20min!)
make generate-billion

# Generate all datasets
make generate-all
```

### Run Benchmarks

**Java:**
```bash
cd java
make build           # Compile the project
make benchmark       # Run quick benchmark (1K rows)
make bench-small     # Run with 10M rows
make bench-medium    # Run with 100M rows
make bench-large     # Run with 500M rows
```

**Go:**
```bash
cd golang
make build           # Compile the project
make run            # Run with default data
make benchmark      # Run all benchmarks
```

### Verify Results

```bash
# Verify results from any implementation
make verify-small    # Verify 10M dataset
make verify-large    # Verify 100M dataset
make verify-billion  # Verify 1B dataset
make verify-all      # Verify all available datasets
```

---

## 💻 Implementations

### Java Implementation

The Java implementation leverages **Java 21** features and focuses on high-performance processing with multiple optimization strategies.

**Key Strategies:**
- **ChunkRead Strategies:**
  - `StandardBuffered` - Traditional buffered reading
  - `MemoryMapped` - Memory-mapped file I/O
  - `ByteBuffered` - Optimized byte buffer handling
  - `Arena` - Arena-based memory management

- **Data Structure Strategies:**
  - `HashTable` - Custom hash table implementation
  - `LinearProbing` - Linear probing hash table with optimizations

**Performance Highlights:**
- Parallel processing with `ForkJoinPool`
- Zero-copy I/O with memory-mapped files
- Custom hash tables for minimal allocations
- Arena-based memory management for reduced GC pressure

[📖 Java Documentation](java/README.md)

### Go Implementation

The Go implementation utilizes **goroutines** and **channels** for concurrent processing with multiple parsing strategies.

**Key Strategies:**
- `basic` - Simple sequential processing
- `batch` - Batch processing with workers
- `mcmp` - Memory-mapped concurrent processing
- `mmap` - Memory-mapped file strategy
- `parse` - Optimized parsing with custom algorithms

**Performance Highlights:**
- Goroutine-based parallelism
- Memory-mapped file support
- Custom byte parsing for minimal allocations
- Concurrent hash map implementations

[📖 Go Documentation](golang/README.md)

---

## 📊 Benchmarks


> **Note:** Actual performance varies based on hardware, JVM settings, and Go version.

### Running Benchmarks

**Quick Benchmark (1K rows):**
```bash
cd java && make benchmark
cd golang && make benchmark
```

**Full Benchmark Suite:**
```bash
# Java
cd java
make bench-small bench-medium bench-large

# Go
cd golang
make benchmark
```

---

## 📚 Documentation

### Data Generation

Generate temperature measurement data for testing:

```bash
# Generate specific sizes
python generate.py 1000000      # 1M rows
python generate.py 10000000     # 10M rows
python generate.py              # 100M rows (default)
python generate.py b            # 1 billion rows

# Using Makefile (recommended)
make generate-tiny              # 1M rows
make generate-small             # 10M rows
make generate-large             # 100M rows
make generate-billion           # 1B rows
```

**Station Data:**
- Uses real weather station names from `stations.json`
- 413 unique weather stations worldwide
- Temperature ranges: -99.9°C to 99.9°C
- Deterministic random generation for reproducibility

### Result Verification

Verify that your implementation produces correct results:

```bash
# Verify specific dataset
python verify.py 10m            # Verify 10M rows
python verify.py 100m           # Verify 100M rows
python verify.py 1b             # Verify 1B rows

# Using Makefile
make verify-small               # Verify 10M dataset
make verify-large               # Verify 100M dataset
make verify-all                 # Verify all datasets
```

The verification script:
1. Reads your implementation's output from `results/`
2. Compares against expected calculations
3. Reports any discrepancies in min/max/mean values

---



## 🛠️ Make Commands Reference

### Root Level Commands

```bash
make help                # Show all available commands
make generate           # Generate 100M rows (default)
make generate-tiny      # Generate 1M rows
make generate-small     # Generate 10M rows
make generate-large     # Generate 100M rows
make generate-billion   # Generate 1B rows
make verify            # Verify results
make clean             # Remove generated data
make clean-all         # Remove all generated files
```

---

## 🤝 Contributing

Contributions are welcome! Here's how you can help:

1. **Add New Strategies** - Implement novel approaches
2. **Optimize Existing Code** - Make it faster!
3. **Add More Languages** - Rust, C++, Python implementations
4. **Improve Documentation** - Help others understand the code
5. **Report Issues** - Found a bug? Let us know!

### Development Workflow

```bash
# Fork and clone the repository
git clone https://github.com/YOUR_USERNAME/onebillion.git

# Create a feature branch
git checkout -b feature/my-optimization

# Make your changes and test
make generate-small
cd java && make bench-small
cd golang && make benchmark

# Verify results
make verify-small

# Commit and push
git commit -m "Add: new optimization strategy"
git push origin feature/my-optimization

# Create a Pull Request
```

---

## 📝 License

This project is open source and available under the [MIT License](LICENSE).

---

## 🏆 Credits

Inspired by the original [1 Billion Row Challenge](https://github.com/gunnarmorling/1brc) by Gunnar Morling.

**Implementations by:** [Utkarsh Priyadarshi](https://github.com/utkarsh5026)

---

## 📞 Support

- **Issues:** [GitHub Issues](https://github.com/utkarsh5026/onebillion/issues)
- **Discussions:** [GitHub Discussions](https://github.com/utkarsh5026/onebillion/discussions)

---

<div align="center">

  **⭐ If you find this project helpful, please give it a star! ⭐**

  Made with ❤️ by a developer who love performance and optimization

</div>