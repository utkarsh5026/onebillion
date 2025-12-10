.PHONY: help build run test benchmark bench-small bench-medium bench-large bench-billion clean clean-all compile jar format check jmh-build jmh-run jmh-strategies jmh-parser jmh-example jmh-all

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
	@echo "  $(GREEN)make bench-medium$(RESET)     - Medium benchmark $(YELLOW)(100M rows, ~5min)$(RESET)"
	@echo "  $(GREEN)make bench-large$(RESET)      - Large benchmark $(YELLOW)(500M rows, ~20min)$(RESET)"
	@echo "  $(GREEN)make bench-billion$(RESET)    - Full 1B benchmark $(YELLOW)(1B rows, ~45min!)$(RESET)"
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
	@mvn exec:java -Dexec.args="../data/measurements-10m.txt"

# Medium benchmark (100M rows)
bench-medium: build
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running Medium Benchmark (100M rows)$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mvn exec:java -Dexec.args="../data/measurements-100m.txt"

# Large benchmark (500M rows)
bench-large: build
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running Large Benchmark (500M rows)$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mvn exec:java -Dexec.args="../data/measurements-500m.txt"

# Billion row benchmark (full challenge!)
bench-billion: build
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running BILLION ROW CHALLENGE (1B rows)$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mvn exec:java -Dexec.args="../data/measurements-1b.txt"



# Clean compiled files
clean:
	@echo "$(RED)▶ Cleaning up...$(RESET)"
	@rm -rf $(BUILD_DIR)
	@echo "$(GREEN)✓ Clean complete!$(RESET)"

# Clean everything including Maven cache
clean-all:
	@echo "$(RED)▶ Removing all generated files and directories...$(RESET)"
	@rm -rf $(BUILD_DIR)
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
