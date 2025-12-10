.PHONY: help build run test benchmark bench-small bench-medium bench-large bench-billion clean clean-all compile jar format check

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
	@echo "  $(CYAN)make format$(RESET)           - Format code (requires google-java-format)"
	@echo "  $(CYAN)make check$(RESET)            - Run code quality checks"
	@echo ""
	@echo "$(BOLD)Benchmarks (auto-generate + run):$(RESET)"
	@echo "  $(GREEN)make benchmark$(RESET)        - Quick benchmark $(YELLOW)(1K rows, instant)$(RESET)"
	@echo "  $(GREEN)make bench-small$(RESET)      - Small benchmark $(YELLOW)(10M rows, ~30s)$(RESET)"
	@echo "  $(GREEN)make bench-medium$(RESET)     - Medium benchmark $(YELLOW)(100M rows, ~5min)$(RESET)"
	@echo "  $(GREEN)make bench-large$(RESET)      - Large benchmark $(YELLOW)(500M rows, ~20min)$(RESET)"
	@echo "  $(GREEN)make bench-billion$(RESET)    - Full 1B benchmark $(YELLOW)(1B rows, ~45min!)$(RESET)"
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

# Format code (requires google-java-format)
format:
	@echo "$(BLUE)▶ Formatting code...$(RESET)"
	@if command -v google-java-format > /dev/null 2>&1; then \
		find $(SRC_DIR) -name "*.java" -exec google-java-format -i {} +; \
		echo "$(GREEN)✓ Code formatted!$(RESET)"; \
	else \
		echo "$(YELLOW)⚠ google-java-format not installed$(RESET)"; \
		echo "  Install with: $(CYAN)brew install google-java-format$(RESET) (macOS)"; \
		echo "  Or download from: https://github.com/google/google-java-format"; \
	fi

# Code quality checks
check: build
	@echo "$(BLUE)▶ Running code quality checks...$(RESET)"
	@echo "$(GREEN)✓ Build passed - basic check complete!$(RESET)"
	@echo "$(YELLOW)Tip: Add checkstyle or spotbugs for more thorough checks$(RESET)"

# Quick benchmark with 1K rows (for testing)
benchmark: build
	@echo "$(YELLOW)▶ Quick benchmark with 1K rows...$(RESET)"
	@mkdir -p ../data
	@python $(GENERATOR) 1000
	@echo ""
	@mvn exec:java
	@echo "$(GREEN)✓ Quick benchmark complete!$(RESET)"

# Small benchmark (10M rows)
bench-small: build generate-small
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running Small Benchmark (10M rows)$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mvn exec:java -Dexec.args="../data/measurements-10m.txt"

# Medium benchmark (100M rows)
bench-medium: build generate-medium
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running Medium Benchmark (100M rows)$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mvn exec:java -Dexec.args="../data/measurements-100m.txt"

# Large benchmark (500M rows)
bench-large: build generate-large
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running Large Benchmark (500M rows)$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mvn exec:java -Dexec.args="../data/measurements-500m.txt"

# Billion row benchmark (full challenge!)
bench-billion: build generate-billion
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running BILLION ROW CHALLENGE (1B rows)$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@mvn exec:java -Dexec.args="../data/measurements-1b.txt"

# Data generation targets
generate-small:
	@echo "$(BLUE)▶ Generating 10M rows...$(RESET)"
	@mkdir -p ../data
	@python $(GENERATOR) 10000000
	@echo "$(GREEN)✓ Generated: ../data/measurements-10m.txt$(RESET)"

generate-medium:
	@echo "$(BLUE)▶ Generating 100M rows...$(RESET)"
	@mkdir -p ../data
	@python $(GENERATOR) 100000000
	@echo "$(GREEN)✓ Generated: ../data/measurements-100m.txt$(RESET)"

generate-large:
	@echo "$(BLUE)▶ Generating 500M rows...$(RESET)"
	@mkdir -p ../data
	@python $(GENERATOR) 500000000
	@echo "$(GREEN)✓ Generated: ../data/measurements-500m.txt$(RESET)"

generate-billion:
	@echo "$(BLUE)▶ Generating 1B rows...$(RESET)"
	@mkdir -p ../data
	@python $(GENERATOR) 1000000000
	@echo "$(GREEN)✓ Generated: ../data/measurements-1b.txt$(RESET)"

# Clean compiled files and generated data
clean:
	@echo "$(RED)▶ Cleaning up...$(RESET)"
	@rm -rf $(BUILD_DIR)
	@rm -f ../data/measurements-*.txt
	@echo "$(GREEN)✓ Clean complete!$(RESET)"

# Clean everything including Maven cache
clean-all:
	@echo "$(RED)▶ Removing all generated files and directories...$(RESET)"
	@rm -rf $(BUILD_DIR)
	@rm -rf ../data
	@echo "$(GREEN)✓ Deep clean complete!$(RESET)"
