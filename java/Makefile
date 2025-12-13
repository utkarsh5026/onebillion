.PHONY: help build run test benchmark bench-small bench-medium bench-large bench-billion clean clean-all compile jar format check jmh-build jmh-run jmh-strategies jmh-parser jmh-example jmh-all profile profile-cpu-only profile-memory-only profile-gc-only profile-all-data profile-jfr profile-jfr-small profile-gc profile-mem profile-cpu profile-async profile-all jmh-profile-gc jmh-profile-stack jmh-profile-perf jmh-profile-async bench-profile-compare

# Color codes (ANSI)
BLUE := \033[1;34m
GREEN := \033[1;32m
YELLOW := \033[1;33m
RED := \033[1;31m
CYAN := \033[1;36m
MAGENTA := \033[1;35m
BOLD := \033[1m
RESET := \033[0m

# Project configuration
MAIN_CLASS := com.onebillion.Main
SRC_DIR := src/main/java
TEST_DIR := src/test/java
BUILD_DIR := target
CLASSES_DIR := $(BUILD_DIR)/classes
TEST_CLASSES_DIR := $(BUILD_DIR)/test-classes
JAR_FILE := $(BUILD_DIR)/onebillion.jar
GENERATOR := ../generate.py

# Java configuration
JAVAC := javac
JAVA := java
JAVAC_FLAGS := -d $(CLASSES_DIR) -sourcepath $(SRC_DIR)
JAVA_FLAGS := -cp $(CLASSES_DIR)

# Default target
help:
	@echo "$(CYAN)╔════════════════════════════════════════════════════════════════════════╗$(RESET)"
	@echo "$(CYAN)║$(RESET)  $(BOLD)One Billion Row Challenge - Java Benchmark Suite$(RESET)$(CYAN)                      ║$(RESET)"
	@echo "$(CYAN)╚════════════════════════════════════════════════════════════════════════╝$(RESET)"
	@echo ""
	@echo "$(BOLD)Build & Run:$(RESET)"
	@echo "  $(GREEN)make build$(RESET)            - Compile all Java source files"
	@echo "  $(GREEN)make compile$(RESET)          - Alias for build"
	@echo "  $(GREEN)make run$(RESET)              - Run benchmark with default data"
	@echo "  $(GREEN)make jar$(RESET)              - Create executable JAR file"
	@echo "  $(GREEN)make test$(RESET)             - Run JUnit tests (if configured)"
	@echo ""
	@echo "$(BOLD)Code Quality:$(RESET)"
	@echo "  $(CYAN)make format$(RESET)           - Format code with Google Java Format (Maven)"
	@echo "  $(CYAN)make check$(RESET)            - Run code quality and formatting checks"
	@echo ""
	@echo "$(BOLD)Benchmarks (auto-generate + run):$(RESET)"
	@echo "  $(GREEN)make benchmark$(RESET)        - Quick benchmark $(YELLOW)(1K rows, instant)$(RESET)"
	@echo "  $(GREEN)make bench-small$(RESET)      - Small benchmark $(YELLOW)(10M rows, ~30s)$(RESET)"
	@echo "  $(GREEN)make bench-small-simd$(RESET) - Small benchmark with SIMD $(YELLOW)(10M rows)$(RESET)"
	@echo "  $(GREEN)make bench-large$(RESET)      - Large benchmark $(YELLOW)(100M rows, ~20min)$(RESET)"
	@echo "  $(GREEN)make bench-large-simd$(RESET) - Large benchmark with SIMD $(YELLOW)(100M rows)$(RESET)"
	@echo "  $(GREEN)make bench-billion$(RESET)    - Full 1B benchmark $(YELLOW)(1B rows, ~45min!)$(RESET)"
	@echo "  $(GREEN)make bench-billion-simd$(RESET) - Full 1B benchmark with SIMD $(YELLOW)(1B rows)$(RESET)"
	@echo ""
	@echo "$(BOLD)Profiling & Performance Analysis (Easy Mode):$(RESET)"
	@echo "  $(YELLOW)make profile$(RESET)          - $(BOLD)Run & show CPU+Memory+GC analysis (RECOMMENDED)$(RESET)"
	@echo "  $(YELLOW)make profile-cpu-only$(RESET) - Run & show CPU hotspots only"
	@echo "  $(YELLOW)make profile-memory-only$(RESET) - Run & show allocation hotspots"
	@echo "  $(YELLOW)make profile-gc-only$(RESET)  - Run & show GC statistics"
	@echo "  $(YELLOW)make profile-all-data$(RESET) - Complete analysis with all metrics"
	@echo ""
	@echo "$(BOLD)Advanced Profiling (raw files for GUI tools):$(RESET)"
	@echo "  $(YELLOW)make profile-jfr$(RESET)      - Generate JFR file (open with JMC)"
	@echo "  $(YELLOW)make profile-async$(RESET)    - Generate flame graphs (async-profiler)"
	@echo ""
	@echo "$(BOLD)JMH Micro-Benchmarks (Go-style profiling):$(RESET)"
	@echo "  $(MAGENTA)make jmh-build$(RESET)        - Build JMH benchmark JAR"
	@echo "  $(MAGENTA)make jmh-run$(RESET)          - Run all JMH benchmarks"
	@echo "  $(MAGENTA)make jmh-strategies$(RESET)   - Benchmark strategy implementations"
	@echo "  $(MAGENTA)make jmh-parser$(RESET)       - Benchmark line parsing"
	@echo "  $(MAGENTA)make jmh-example$(RESET)      - Run example benchmarks"
	@echo "  $(MAGENTA)make jmh-all$(RESET)          - Run all JMH benchmarks with reports"
	@echo ""
	@echo "$(BOLD)Maven Support:$(RESET)"
	@echo "  $(BLUE)mvn compile$(RESET)           - Compile with Maven"
	@echo "  $(BLUE)mvn test$(RESET)              - Run tests with Maven"
	@echo "  $(BLUE)mvn package$(RESET)           - Create JAR with Maven"
	@echo "  $(BLUE)mvn exec:java$(RESET)         - Run with Maven"
	@echo ""
	@echo "$(BOLD)Cleanup:$(RESET)"
	@echo "  $(RED)make clean$(RESET)            - Remove compiled files and generated data"
	@echo "  $(RED)make clean-all$(RESET)        - Remove everything (target, cache, data)"
	@echo ""

# Create necessary directories
$(CLASSES_DIR):
	@mkdir -p $(CLASSES_DIR)

$(TEST_CLASSES_DIR):
	@mkdir -p $(TEST_CLASSES_DIR)

# Build/compile the project
build:
	@echo "$(BLUE)▶ Compiling Java sources with Maven...$(RESET)"
	@mvn clean compile
	@echo "$(GREEN)✓ Build complete!$(RESET) → $(CYAN)$(BUILD_DIR)$(RESET)"

compile: build

# Run with default data
run: build
	@echo "$(BLUE)▶ Running benchmark with default data...$(RESET)"
	@mvn exec:java

# Create executable JAR
jar: build
	@echo "$(BLUE)▶ Creating JAR file with Maven...$(RESET)"
	@mvn package
	@echo "$(GREEN)✓ JAR created!$(RESET) → $(CYAN)$(BUILD_DIR)/onebillion-challenge.jar$(RESET)"
	@echo "$(YELLOW)Run with:$(RESET) java -jar $(BUILD_DIR)/onebillion-challenge.jar"

# Run tests (basic setup, expand as needed)
test: build
	@echo "$(BLUE)▶ Running tests...$(RESET)"
	@if [ -d "$(TEST_DIR)" ] && [ -n "$$(find $(TEST_DIR) -name '*.java' 2>/dev/null)" ]; then \
		echo "$(YELLOW)Compiling and running tests...$(RESET)"; \
		$(JAVAC) -d $(TEST_CLASSES_DIR) -cp $(CLASSES_DIR) $(shell find $(TEST_DIR) -name "*.java" 2>/dev/null || echo ""); \
		echo "$(GREEN)✓ Tests compiled!$(RESET)"; \
	else \
		echo "$(YELLOW)No tests found in $(TEST_DIR)$(RESET)"; \
	fi

# Format code with Google Java Format (via Maven plugin)
format:
	@echo "$(BLUE)▶ Formatting code with Google Java Format...$(RESET)"
	@mvn fmt:format
	@echo "$(GREEN)✓ Code formatted!$(RESET)"

# Code quality checks
check: build
	@echo "$(BLUE)▶ Running code quality checks...$(RESET)"
	@mvn fmt:check
	@echo "$(GREEN)✓ Build passed - basic check complete!$(RESET)"
	@echo "$(GREEN)✓ Code formatting check passed!$(RESET)"

# Quick benchmark with 1K rows (for testing)
benchmark: build
	@echo "$(YELLOW)▶ Quick benchmark with 1K rows...$(RESET)"
	@mkdir -p ../data
	@python $(GENERATOR) 1000
	@echo ""
	@mvn exec:java
	@echo "$(GREEN)✓ Quick benchmark complete!$(RESET)"

# Small benchmark (10M rows)
bench-small: build
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running Small Benchmark (10M rows)$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@java --add-modules jdk.incubator.vector -cp target/classes com.onebillion.Main ../data/measurements-10m.txt

# Small benchmark with SIMD (10M rows)
bench-small-simd: build
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running Small Benchmark with SIMD (10M rows)$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@java --add-modules jdk.incubator.vector -cp target/classes com.onebillion.Main --simd ../data/measurements-10m.txt

# Medium benchmark (100M rows)
bench-large: build
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running Medium Benchmark (100M rows)$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@java --add-modules jdk.incubator.vector -cp target/classes com.onebillion.Main ../data/measurements-100m.txt

# Medium benchmark with SIMD (100M rows)
bench-large-simd: build
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running Medium Benchmark with SIMD (100M rows)$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@java --add-modules jdk.incubator.vector -cp target/classes com.onebillion.Main --simd ../data/measurements-100m.txt

# Billion row benchmark (full challenge!)
bench-billion: build
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running BILLION ROW CHALLENGE (1B rows)$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mvn exec:java -Dexec.args="../data/measurements-1b.txt"

# Billion row benchmark with SIMD (full challenge!)
bench-billion-simd: build
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running BILLION ROW CHALLENGE with SIMD (1B rows)$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mvn exec:java -Dexec.args="--simd ../data/measurements-1b.txt"

# ═══════════════════════════════════════════════════════════════════
# Profiling Targets - Easy Mode (Run + Analyze in One Command)
# ═══════════════════════════════════════════════════════════════════

# Main profiling command - does everything and shows results immediately
profile: build
	@echo ""
	@echo "$(CYAN)$(BOLD)╔═══════════════════════════════════════════════════════════════════╗$(RESET)"
	@echo "$(CYAN)$(BOLD)║  Complete Performance Profile - CPU + Memory + GC                ║$(RESET)"
	@echo "$(CYAN)$(BOLD)╚═══════════════════════════════════════════════════════════════════╝$(RESET)"
	@echo ""
	@mkdir -p profiles
	@echo "$(YELLOW)⏱️  Running benchmark with profiling enabled...$(RESET)"
	@echo ""
	@mvn exec:java \
		-Dexec.args="../data/measurements-10m.txt" \
		-Dexec.jvmArgs="-XX:StartFlightRecording=filename=profiles/profile.jfr,settings=profile,dumponexit=true -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:FlightRecorderOptions=stackdepth=1024"
	@echo ""
	@echo "$(GREEN)✓ Profiling complete! Analyzing results...$(RESET)"
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  📊 PERFORMANCE SUMMARY$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@jfr summary profiles/profile.jfr 2>/dev/null || echo "$(YELLOW)JFR summary not available (JDK 11+ required)$(RESET)"
	@echo ""
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo "$(YELLOW)$(BOLD)  🔥 CPU HOTSPOTS - Top Methods Consuming CPU$(RESET)"
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@jfr print --events jdk.ExecutionSample profiles/profile.jfr 2>/dev/null | head -60 || echo "$(YELLOW)Use: jfr print --events jdk.ExecutionSample profiles/profile.jfr$(RESET)"
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  🧠 MEMORY ALLOCATION HOTSPOTS$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@jfr print --events jdk.ObjectAllocationSample profiles/profile.jfr 2>/dev/null | head -60 || echo "$(YELLOW)Use: jfr print --events jdk.ObjectAllocationSample profiles/profile.jfr$(RESET)"
	@echo ""
	@echo "$(BLUE)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo "$(BLUE)$(BOLD)  🗑️  GARBAGE COLLECTION STATISTICS$(RESET)"
	@echo "$(BLUE)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@jfr print --events jdk.GarbageCollection profiles/profile.jfr 2>/dev/null | tail -30 || echo "$(YELLOW)Use: jfr print --events jdk.GarbageCollection profiles/profile.jfr$(RESET)"
	@echo ""
	@echo "$(GREEN)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo "$(GREEN)✓ Profile saved to: $(CYAN)profiles/profile.jfr$(RESET)"
	@echo "$(YELLOW)💡 For detailed GUI analysis run: $(CYAN)jmc -open profiles/profile.jfr$(RESET)"
	@echo "$(GREEN)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo ""

# CPU-only profiling with analysis printed to terminal
profile-cpu-only: build
	@echo ""
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo "$(YELLOW)$(BOLD)  🔥 CPU Profiling - Finding Performance Bottlenecks$(RESET)"
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mkdir -p profiles
	@echo "$(CYAN)Running with CPU profiling...$(RESET)"
	@mvn exec:java \
		-Dexec.args="../data/measurements-10m.txt" \
		-Dexec.jvmArgs="-XX:StartFlightRecording=filename=profiles/cpu.jfr,settings=profile,dumponexit=true -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:FlightRecorderOptions=stackdepth=1024"
	@echo ""
	@echo "$(CYAN)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@echo "$(CYAN)$(BOLD)  Top 25 CPU Consuming Methods$(RESET)"
	@echo "$(CYAN)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@jfr print --events jdk.ExecutionSample profiles/cpu.jfr 2>/dev/null | head -80 || jfr summary profiles/cpu.jfr
	@echo ""
	@echo "$(GREEN)✓ CPU profile saved: $(CYAN)profiles/cpu.jfr$(RESET)"
	@echo ""

# Memory allocation profiling with analysis
profile-memory-only: build
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  🧠 Memory Allocation Profiling$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mkdir -p profiles
	@echo "$(CYAN)Running with memory profiling...$(RESET)"
	@mvn exec:java \
		-Dexec.args="../data/measurements-10m.txt" \
		-Dexec.jvmArgs="-XX:StartFlightRecording=filename=profiles/memory.jfr,settings=profile,dumponexit=true -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints"
	@echo ""
	@echo "$(CYAN)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@echo "$(CYAN)$(BOLD)  Top Allocation Sites (Objects Created)$(RESET)"
	@echo "$(CYAN)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@jfr print --events jdk.ObjectAllocationSample profiles/memory.jfr 2>/dev/null | head -80 || jfr summary profiles/memory.jfr
	@echo ""
	@echo "$(GREEN)✓ Memory profile saved: $(CYAN)profiles/memory.jfr$(RESET)"
	@echo ""

# GC-only profiling with analysis
profile-gc-only: build
	@echo ""
	@echo "$(BLUE)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo "$(BLUE)$(BOLD)  🗑️  Garbage Collection Profiling$(RESET)"
	@echo "$(BLUE)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mkdir -p profiles
	@echo "$(CYAN)Running with GC profiling...$(RESET)"
	@mvn exec:java \
		-Dexec.args="../data/measurements-10m.txt" \
		-Dexec.jvmArgs="-Xlog:gc*:file=profiles/gc.log:time,level,tags -XX:+UnlockDiagnosticVMOptions"
	@echo ""
	@echo "$(CYAN)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@echo "$(CYAN)$(BOLD)  GC Statistics & Pauses$(RESET)"
	@echo "$(CYAN)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@cat profiles/gc.log | grep -E "(GC|pause|Heap)" || cat profiles/gc.log | tail -40
	@echo ""
	@echo "$(GREEN)✓ GC log saved: $(CYAN)profiles/gc.log$(RESET)"
	@echo "$(YELLOW)💡 Upload to https://gceasy.io for visual GC analysis$(RESET)"
	@echo ""

# Complete analysis with all data points
profile-all-data: build
	@echo ""
	@echo "$(MAGENTA)$(BOLD)╔═══════════════════════════════════════════════════════════════════╗$(RESET)"
	@echo "$(MAGENTA)$(BOLD)║  COMPLETE PERFORMANCE ANALYSIS - All Metrics & Details           ║$(RESET)"
	@echo "$(MAGENTA)$(BOLD)╚═══════════════════════════════════════════════════════════════════╝$(RESET)"
	@echo ""
	@mkdir -p profiles
	@echo "$(CYAN)Running comprehensive profiling (this may take a moment)...$(RESET)"
	@echo ""
	@mvn exec:java \
		-Dexec.args="../data/measurements-10m.txt" \
		-Dexec.jvmArgs="-XX:StartFlightRecording=filename=profiles/complete.jfr,settings=profile,dumponexit=true -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -Xlog:gc*:file=profiles/gc-complete.log:time,level,tags -XX:FlightRecorderOptions=stackdepth=1024"
	@echo ""
	@echo "$(CYAN)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@echo "$(CYAN)$(BOLD)  📊 Overall Summary$(RESET)"
	@echo "$(CYAN)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@jfr summary profiles/complete.jfr 2>/dev/null || echo "Detailed data in complete.jfr"
	@echo ""
	@echo "$(YELLOW)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@echo "$(YELLOW)$(BOLD)  🔥 CPU Hotspots$(RESET)"
	@echo "$(YELLOW)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@jfr print --events jdk.ExecutionSample profiles/complete.jfr 2>/dev/null | head -50 || echo "Data in complete.jfr"
	@echo ""
	@echo "$(MAGENTA)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  🧠 Allocation Hotspots$(RESET)"
	@echo "$(MAGENTA)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@jfr print --events jdk.ObjectAllocationSample profiles/complete.jfr 2>/dev/null | head -50 || echo "Data in complete.jfr"
	@echo ""
	@echo "$(BLUE)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@echo "$(BLUE)$(BOLD)  🗑️  GC Summary$(RESET)"
	@echo "$(BLUE)$(BOLD)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(RESET)"
	@jfr print --events jdk.GarbageCollection profiles/complete.jfr 2>/dev/null | tail -20 || cat profiles/gc-complete.log | tail -30
	@echo ""
	@echo "$(GREEN)$(BOLD)════════════════════════════════════════════════════════════════════$(RESET)"
	@echo "$(GREEN)✓ Complete profile saved: $(CYAN)profiles/complete.jfr$(RESET)"
	@echo "$(GREEN)✓ GC log saved: $(CYAN)profiles/gc-complete.log$(RESET)"
	@echo "$(YELLOW)💡 For GUI analysis: $(CYAN)jmc -open profiles/complete.jfr$(RESET)"
	@echo "$(GREEN)$(BOLD)════════════════════════════════════════════════════════════════════$(RESET)"
	@echo ""

# ═══════════════════════════════════════════════════════════════════
# Advanced Profiling Targets (Raw Files for External Tools)
# ═══════════════════════════════════════════════════════════════════

# Profile with Java Flight Recorder (JFR) - Modern, built-in profiler
profile-jfr: build
	@echo ""
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(YELLOW)$(BOLD)  Profiling with Java Flight Recorder$(RESET)"
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mkdir -p profiles
	@echo "$(CYAN)Generating JFR profile...$(RESET)"
	@mvn exec:java \
		-Dexec.args="../data/measurements-10m.txt" \
		-Dexec.jvmArgs="-XX:StartFlightRecording=filename=profiles/profile.jfr,settings=profile,dumponexit=true -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints"
	@echo ""
	@echo "$(GREEN)✓ JFR profile created!$(RESET) → $(CYAN)profiles/profile.jfr$(RESET)"
	@echo ""
	@echo "$(BOLD)To analyze the profile:$(RESET)"
	@echo "  1. Open with JDK Mission Control: $(CYAN)jmc -open profiles/profile.jfr$(RESET)"
	@echo "  2. Or use jfr CLI: $(CYAN)jfr print --events CPULoad,GarbageCollection profiles/profile.jfr$(RESET)"
	@echo ""

# Profile small benchmark with JFR
profile-jfr-small: build
	@echo ""
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(YELLOW)$(BOLD)  JFR Profiling - 10M Rows$(RESET)"
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mkdir -p profiles
	@mvn exec:java \
		-Dexec.args="../data/measurements-10m.txt" \
		-Dexec.jvmArgs="-XX:StartFlightRecording=filename=profiles/profile-10m.jfr,settings=profile,dumponexit=true"
	@echo "$(GREEN)✓ Profile saved:$(RESET) profiles/profile-10m.jfr"

# Profile with detailed GC logging
profile-gc: build
	@echo ""
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(YELLOW)$(BOLD)  Profiling Garbage Collection$(RESET)"
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mkdir -p profiles
	@mvn exec:java \
		-Dexec.args="../data/measurements-10m.txt" \
		-Dexec.jvmArgs="-Xlog:gc*:file=profiles/gc.log:time,level,tags -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
	@echo ""
	@echo "$(GREEN)✓ GC log created!$(RESET) → $(CYAN)profiles/gc.log$(RESET)"
	@echo ""
	@echo "$(BOLD)Analyze GC log with:$(RESET)"
	@echo "  • GCViewer: https://github.com/chewiebug/GCViewer"
	@echo "  • GCeasy: https://gceasy.io"
	@echo ""

# Profile memory allocation
profile-mem: build
	@echo ""
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(YELLOW)$(BOLD)  Profiling Memory Allocation$(RESET)"
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mkdir -p profiles
	@mvn exec:java \
		-Dexec.args="../data/measurements-10m.txt" \
		-Dexec.jvmArgs="-Xlog:gc*=info:file=profiles/memory.log -XX:+PrintGCDetails -XX:+PrintHeapAtGC -XX:+UnlockDiagnosticVMOptions -XX:NativeMemoryTracking=summary"
	@echo ""
	@echo "$(GREEN)✓ Memory profile created!$(RESET) → $(CYAN)profiles/memory.log$(RESET)"

# CPU profiling with JFR (lightweight)
profile-cpu: build
	@echo ""
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(YELLOW)$(BOLD)  CPU Profiling with JFR$(RESET)"
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mkdir -p profiles
	@mvn exec:java \
		-Dexec.args="../data/measurements-10m.txt" \
		-Dexec.jvmArgs="-XX:StartFlightRecording=filename=profiles/cpu.jfr,settings=profile,dumponexit=true -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:FlightRecorderOptions=stackdepth=1024"
	@echo ""
	@echo "$(GREEN)✓ CPU profile created!$(RESET) → $(CYAN)profiles/cpu.jfr$(RESET)"
	@echo ""
	@echo "$(BOLD)Quick CPU analysis:$(RESET)"
	@echo "  jfr print --events jdk.ExecutionSample profiles/cpu.jfr | head -50"
	@echo ""

# Profile with async-profiler (requires async-profiler installation)
profile-async: build
	@echo ""
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(YELLOW)$(BOLD)  Profiling with async-profiler (Flame Graphs)$(RESET)"
	@echo "$(YELLOW)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mkdir -p profiles
	@if [ -d "/opt/async-profiler" ] || [ -d "$$HOME/async-profiler" ]; then \
		echo "$(CYAN)Running with async-profiler...$(RESET)"; \
		ASYNC_PROFILER=$$(find /opt $$HOME -name "libasyncProfiler.so" 2>/dev/null | head -1); \
		if [ -n "$$ASYNC_PROFILER" ]; then \
			mvn exec:java \
				-Dexec.args="../data/measurements-10m.txt" \
				-Dexec.jvmArgs="-agentpath:$$ASYNC_PROFILER=start,event=cpu,file=profiles/flamegraph.html"; \
			echo "$(GREEN)✓ Flame graph created!$(RESET) → $(CYAN)profiles/flamegraph.html$(RESET)"; \
		else \
			echo "$(RED)async-profiler library not found$(RESET)"; \
			echo "Download from: https://github.com/async-profiler/async-profiler/releases"; \
		fi \
	else \
		echo "$(YELLOW)async-profiler not installed$(RESET)"; \
		echo ""; \
		echo "$(BOLD)To install async-profiler:$(RESET)"; \
		echo "  1. Download: $(CYAN)https://github.com/async-profiler/async-profiler/releases$(RESET)"; \
		echo "  2. Extract to /opt/async-profiler or ~/async-profiler"; \
		echo ""; \
		echo "$(YELLOW)Using JFR instead...$(RESET)"; \
		$(MAKE) profile-jfr; \
	fi

# Profile comparison - run multiple profiles
profile-all: build
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running Complete Profiling Suite$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@$(MAKE) profile-jfr-small
	@$(MAKE) profile-gc
	@echo ""
	@echo "$(GREEN)✓ All profiles complete!$(RESET)"
	@echo "$(CYAN)Check the profiles/ directory for results$(RESET)"

# ═══════════════════════════════════════════════════════════════════
# JMH Profiling Integration
# ═══════════════════════════════════════════════════════════════════

# Run JMH with built-in profilers
jmh-profile-gc: jmh-build
	@echo "$(YELLOW)Running JMH with GC profiler...$(RESET)"
	@java -jar $(BUILD_DIR)/benchmarks.jar -prof gc

jmh-profile-stack: jmh-build
	@echo "$(YELLOW)Running JMH with stack profiler...$(RESET)"
	@java -jar $(BUILD_DIR)/benchmarks.jar -prof stack

jmh-profile-perf: jmh-build
	@echo "$(YELLOW)Running JMH with perf profiler (Linux only)...$(RESET)"
	@java -jar $(BUILD_DIR)/benchmarks.jar -prof perfnorm

jmh-profile-async: jmh-build
	@echo "$(YELLOW)Running JMH with async-profiler...$(RESET)"
	@mkdir -p profiles/jmh
	@java -jar $(BUILD_DIR)/benchmarks.jar -prof async:output=flamegraph;dir=profiles/jmh

# Clean compiled files
clean:
	@echo "$(RED)▶ Cleaning up...$(RESET)"
	@rm -rf $(BUILD_DIR)
	@echo "$(GREEN)✓ Clean complete!$(RESET)"

# Clean everything including Maven cache
clean-all:
	@echo "$(RED)▶ Removing all generated files and directories...$(RESET)"
	@rm -rf $(BUILD_DIR)
	@rm -rf profiles
	@rm -rf reports
	@echo "$(GREEN)✓ Deep clean complete!$(RESET)"

# ═══════════════════════════════════════════════════════════════════
# JMH Benchmarking Targets (Go-style benchmarking for Java)
# ═══════════════════════════════════════════════════════════════════

# Build JMH benchmark JAR
jmh-build:
	@echo "$(MAGENTA)▶ Building JMH benchmark JAR...$(RESET)"
	@mvn clean package -Pbenchmarks -DskipTests
	@echo "$(GREEN)✓ JMH benchmarks built!$(RESET) → $(CYAN)$(BUILD_DIR)/benchmarks.jar$(RESET)"
	@echo ""
	@echo "$(YELLOW)Run with:$(RESET) java -jar $(BUILD_DIR)/benchmarks.jar"
	@echo ""

# Run all JMH benchmarks
jmh-run: jmh-build
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running All JMH Benchmarks$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@java -jar $(BUILD_DIR)/benchmarks.jar
	@echo ""
	@echo "$(GREEN)✓ All benchmarks complete!$(RESET)"

# Benchmark strategy implementations
jmh-strategies: jmh-build
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Benchmarking Strategy Implementations$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@java -jar $(BUILD_DIR)/benchmarks.jar StrategyBenchmark
	@echo ""
	@echo "$(GREEN)✓ Strategy benchmarks complete!$(RESET)"

# Benchmark line parser
jmh-parser: jmh-build
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Benchmarking Line Parser$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@java -jar $(BUILD_DIR)/benchmarks.jar LineParserBenchmark
	@echo ""
	@echo "$(GREEN)✓ Parser benchmarks complete!$(RESET)"

# Run example benchmarks
jmh-example: jmh-build
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running Example Benchmarks$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@java -jar $(BUILD_DIR)/benchmarks.jar ExampleBenchmark
	@echo ""
	@echo "$(GREEN)✓ Example benchmarks complete!$(RESET)"

# Run all benchmarks with detailed reports
jmh-all: jmh-build
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running Complete JMH Benchmark Suite$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mkdir -p reports
	@java -jar $(BUILD_DIR)/benchmarks.jar -rf json -rff reports/jmh-results.json
	@echo ""
	@echo "$(GREEN)✓ All benchmarks complete!$(RESET)"
	@echo "$(CYAN)Results saved to:$(RESET) reports/jmh-results.json"
	@echo ""

# ═══════════════════════════════════════════════════════════════════
# Optimization Verification Target
# ═══════════════════════════════════════════════════════════════════

# Run benchmarks with detailed profiling to verify optimizations
bench-profile-compare: build
	@echo ""
	@echo "$(MAGENTA)$(BOLD)╔═══════════════════════════════════════════════════════════════════╗$(RESET)"
	@echo "$(MAGENTA)$(BOLD)║  Benchmark with Optimization Analysis - 10M & 100M rows          ║$(RESET)"
	@echo "$(MAGENTA)$(BOLD)╚═══════════════════════════════════════════════════════════════════╝$(RESET)"
	@echo ""
	@mkdir -p profiles
	@echo "$(CYAN)$(BOLD)━━━ Phase 1: 10M Row Benchmark with GC Profiling ━━━$(RESET)"
	@echo ""
	@mvn exec:java \
		-Dexec.args="../data/measurements-10m.txt" \
		-Dexec.jvmArgs="-Xlog:gc*:file=profiles/gc-10m.log:time,level,tags -XX:+UnlockDiagnosticVMOptions"
	@echo ""
	@echo "$(CYAN)$(BOLD)━━━ Phase 2: 100M Row Benchmark with GC Profiling ━━━$(RESET)"
	@echo ""
	@mvn exec:java \
		-Dexec.args="../data/measurements-100m.txt" \
		-Dexec.jvmArgs="-Xlog:gc*:file=profiles/gc-100m.log:time,level,tags -XX:+UnlockDiagnosticVMOptions"
	@echo ""
	@echo "$(GREEN)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo "$(GREEN)$(BOLD)  📊 GC ANALYSIS SUMMARY$(RESET)"
	@echo "$(GREEN)$(BOLD)═══════════════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@echo "$(YELLOW)10M Rows - GC Statistics:$(RESET)"
	@grep -E "GC\([0-9]+\)" profiles/gc-10m.log | tail -10 || echo "See profiles/gc-10m.log for details"
	@echo ""
	@echo "$(YELLOW)100M Rows - GC Statistics:$(RESET)"
	@grep -E "GC\([0-9]+\)" profiles/gc-100m.log | tail -10 || echo "See profiles/gc-100m.log for details"
	@echo ""
	@echo "$(GREEN)✓ Profile logs saved:$(RESET)"
	@echo "  • $(CYAN)profiles/gc-10m.log$(RESET)"
	@echo "  • $(CYAN)profiles/gc-100m.log$(RESET)"
	@echo ""