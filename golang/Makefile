.PHONY: help build run test benchmark bench-small bench-medium bench-large bench-billion generate-small generate-medium generate-large generate-billion clean clean-all

# Color codes (ANSI)
BLUE := \033[1;34m
GREEN := \033[1;32m
YELLOW := \033[1;33m
RED := \033[1;31m
CYAN := \033[1;36m
MAGENTA := \033[1;35m
BOLD := \033[1m
RESET := \033[0m

# Binary name
BINARY := benchmark
GENERATOR := ../generate.py

# Default target
help:
	@echo "$(CYAN)╔════════════════════════════════════════════════════════════════════════╗$(RESET)"
	@echo "$(CYAN)║$(RESET)  $(BOLD)One Billion Row Challenge - Go Benchmark Suite$(RESET)$(CYAN)                       ║$(RESET)"
	@echo "$(CYAN)╚════════════════════════════════════════════════════════════════════════╝$(RESET)"
	@echo ""
	@echo "$(BOLD)Build & Run:$(RESET)"
	@echo "  $(GREEN)make build$(RESET)            - Compile the benchmark binary"
	@echo "  $(GREEN)make run$(RESET)              - Run benchmark with default data"
	@echo "  $(GREEN)make test$(RESET)             - Run Go tests"
	@echo ""
	@echo "$(BOLD)Benchmarks (auto-generate + run):$(RESET)"
	@echo "  $(GREEN)make benchmark$(RESET)        - Quick benchmark $(YELLOW)(1K rows, instant)$(RESET)"
	@echo "  $(GREEN)make bench-small$(RESET)      - Small benchmark $(YELLOW)(10M rows, ~30s)$(RESET)"
	@echo "  $(GREEN)make bench-medium$(RESET)     - Medium benchmark $(YELLOW)(100M rows, ~5min)$(RESET)"
	@echo "  $(GREEN)make bench-large$(RESET)      - Large benchmark $(YELLOW)(500M rows, ~20min)$(RESET)"
	@echo "  $(GREEN)make bench-billion$(RESET)    - Full 1B benchmark $(YELLOW)(1B rows, ~45min!)$(RESET)"
	@echo ""
	@echo "$(BOLD)Data Generation:$(RESET)"
	@echo "  $(GREEN)make generate-small$(RESET)   - Generate 10M rows $(YELLOW)(~140MB)$(RESET)"
	@echo "  $(GREEN)make generate-medium$(RESET)  - Generate 100M rows $(YELLOW)(~1.4GB)$(RESET)"
	@echo "  $(GREEN)make generate-large$(RESET)   - Generate 500M rows $(YELLOW)(~7GB)$(RESET)"
	@echo "  $(GREEN)make generate-billion$(RESET) - Generate 1B rows $(YELLOW)(~14GB)$(RESET)"
	@echo ""
	@echo "$(BOLD)Cleanup:$(RESET)"
	@echo "  $(RED)make clean$(RESET)            - Remove binary and generated data"
	@echo "  $(RED)make clean-all$(RESET)        - Remove everything (binary, data, cache)"
	@echo ""

# Build the binary
build:
	@echo "$(BLUE)▶ Building benchmark binary...$(RESET)"
	@go build -o $(BINARY).exe main.go
	@echo "$(GREEN)✓ Build complete!$(RESET) → $(CYAN)$(BINARY).exe$(RESET)"

# Run with default data (will use existing data/measurements.txt or fail gracefully)
run: build
	@echo "$(BLUE)▶ Running benchmark with default data...$(RESET)"
	@./$(BINARY).exe

# Run Go tests
test:
	@echo "$(BLUE)▶ Running Go tests...$(RESET)"
	@go test -v ./...
	@echo "$(GREEN)✓ Tests complete!$(RESET)"

# Quick benchmark with 1K rows (for testing)
benchmark: build
	@echo "$(YELLOW)▶ Quick benchmark with 1K rows...$(RESET)"
	@mkdir -p data
	@python $(GENERATOR) 1000
	@echo ""
	@./$(BINARY).exe
	@echo "$(GREEN)✓ Quick benchmark complete!$(RESET)"

# Small benchmark (10M rows)
bench-small: build generate-small
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running Small Benchmark (10M rows)$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@./$(BINARY).exe

# Medium benchmark (100M rows)
bench-medium: build generate-medium
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running Medium Benchmark (100M rows)$(RESET)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@./$(BINARY).exe

# Large benchmark (500M rows)
bench-large: build generate-large
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running Large Benchmark (500M rows)$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@./$(BINARY).exe

# Billion row benchmark (full challenge!)
bench-billion: build generate-billion
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running BILLION ROW CHALLENGE (1B rows)$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@./$(BINARY).exe

# Generate test data of different sizes
generate-small:
	@echo "$(BLUE)▶ Generating 10M rows...$(RESET)"
	@mkdir -p data
	@python $(GENERATOR) 10000000
	@echo "$(GREEN)✓ Data generation complete!$(RESET)"

generate-medium:
	@echo "$(BLUE)▶ Generating 100M rows...$(RESET)"
	@mkdir -p data
	@python $(GENERATOR) 100000000
	@echo "$(GREEN)✓ Data generation complete!$(RESET)"

generate-large:
	@echo "$(BLUE)▶ Generating 500M rows...$(RESET)"
	@mkdir -p data
	@python $(GENERATOR) 500000000
	@echo "$(GREEN)✓ Data generation complete!$(RESET)"

generate-billion:
	@echo "$(MAGENTA)▶ Generating 1 BILLION rows $(YELLOW)(this will take a while!)$(RESET)"
	@mkdir -p data
	@python $(GENERATOR) b
	@echo "$(GREEN)✓ Data generation complete!$(RESET)"

# Clean generated files and binary
clean:
	@echo "$(RED)▶ Cleaning up...$(RESET)"
	@rm -f $(BINARY).exe
	@rm -f data/measurements-*.txt
	@echo "$(GREEN)✓ Clean complete!$(RESET)"

# Clean everything including data directory
clean-all:
	@echo "$(RED)▶ Removing all generated files and directories...$(RESET)"
	@rm -f $(BINARY).exe
	@rm -rf data
	@go clean -cache -testcache -modcache
	@echo "$(GREEN)✓ Deep clean complete!$(RESET)"
