.PHONY: help build run test benchmark bench-small bench-medium bench-large bench-billion generate-small generate-medium generate-large generate-billion force-generate-small force-generate-medium force-generate-large force-generate-billion profile profile-cpu profile-mem pprof-cpu pprof-mem pprof-web clean clean-all clean-profiles fmt vet lint modernize tidy check

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

# Profiling files
CPU_PROFILE := cpu.prof
MEM_PROFILE := mem.prof

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
	@echo "$(BOLD)Code Quality:$(RESET)"
	@echo "  $(CYAN)make fmt$(RESET)              - Format code using go fmt"
	@echo "  $(CYAN)make vet$(RESET)              - Run go vet for static analysis"
	@echo "  $(CYAN)make lint$(RESET)             - Run golangci-lint"
	@echo "  $(CYAN)make modernize$(RESET)        - Modernize Go code with latest idioms"
	@echo "  $(CYAN)make tidy$(RESET)             - Clean up go.mod and go.sum"
	@echo "  $(CYAN)make check$(RESET)            - Run all checks (fmt, vet, lint, test)"
	@echo ""
	@echo "$(BOLD)Benchmarks (auto-generate + run):$(RESET)"
	@echo "  $(GREEN)make benchmark$(RESET)        - Quick benchmark $(YELLOW)(1K rows, instant)$(RESET)"
	@echo "  $(GREEN)make bench-small$(RESET)      - Small benchmark $(YELLOW)(10M rows, ~30s)$(RESET)"
	@echo "  $(GREEN)make bench-medium$(RESET)     - Medium benchmark $(YELLOW)(100M rows, ~5min)$(RESET)"
	@echo "  $(GREEN)make bench-large$(RESET)      - Large benchmark $(YELLOW)(500M rows, ~20min)$(RESET)"
	@echo "  $(GREEN)make bench-billion$(RESET)    - Full 1B benchmark $(YELLOW)(1B rows, ~45min!)$(RESET)"
	@echo ""
	@echo "$(BOLD)Data Generation (skips if exists):$(RESET)"
	@echo "  $(GREEN)make generate-small$(RESET)   - Generate 10M rows $(YELLOW)(~140MB)$(RESET)"
	@echo "  $(GREEN)make generate-medium$(RESET)  - Generate 100M rows $(YELLOW)(~1.4GB)$(RESET)"
	@echo "  $(GREEN)make generate-large$(RESET)   - Generate 500M rows $(YELLOW)(~7GB)$(RESET)"
	@echo "  $(GREEN)make generate-billion$(RESET) - Generate 1B rows $(YELLOW)(~14GB)$(RESET)"
	@echo ""
	@echo "$(BOLD)Performance Profiling:$(RESET)"
	@echo "  $(MAGENTA)make profile$(RESET)          - Profile with CPU + memory profiling"
	@echo "  $(MAGENTA)make profile-cpu$(RESET)      - CPU profiling only"
	@echo "  $(MAGENTA)make profile-mem$(RESET)      - Memory profiling only"
	@echo "  $(MAGENTA)make pprof-cpu$(RESET)        - Analyze CPU profile (interactive)"
	@echo "  $(MAGENTA)make pprof-mem$(RESET)        - Analyze memory profile (interactive)"
	@echo "  $(MAGENTA)make pprof-web$(RESET)        - Open profile in web browser"
	@echo ""
	@echo "$(BOLD)Cleanup:$(RESET)"
	@echo "  $(RED)make clean$(RESET)            - Remove binary and generated data"
	@echo "  $(RED)make clean-profiles$(RESET)   - Remove profiling data"
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

# Format code
fmt:
	@echo "$(BLUE)▶ Formatting code...$(RESET)"
	@go fmt ./...
	@echo "$(GREEN)✓ Code formatted!$(RESET)"

# Run go vet
vet:
	@echo "$(BLUE)▶ Running go vet...$(RESET)"
	@go vet ./...
	@echo "$(GREEN)✓ Vet check passed!$(RESET)"

# Run golangci-lint
lint:
	@echo "$(BLUE)▶ Running golangci-lint...$(RESET)"
	@if command -v golangci-lint > /dev/null 2>&1; then \
		golangci-lint run ./...; \
		echo "$(GREEN)✓ Lint check passed!$(RESET)"; \
	else \
		echo "$(YELLOW)⚠ golangci-lint not installed. Install with:$(RESET)"; \
		echo "  $(CYAN)go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest$(RESET)"; \
		echo "$(YELLOW)⚠ Falling back to basic checks...$(RESET)"; \
		go vet ./...; \
		echo "$(GREEN)✓ Basic vet check passed!$(RESET)"; \
	fi

# Modernize Go code with latest idioms
modernize:
	@echo "$(BLUE)▶ Modernizing Go code...$(RESET)"
	@go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...
	@echo "$(GREEN)✓ Code modernization complete!$(RESET)"

# Tidy up dependencies
tidy:
	@echo "$(BLUE)▶ Tidying dependencies...$(RESET)"
	@go mod tidy
	@echo "$(GREEN)✓ Dependencies tidied!$(RESET)"

# Run all checks
check: fmt vet lint test
	@echo ""
	@echo "$(GREEN)$(BOLD)✓ All checks passed!$(RESET)"
	@echo ""

# Quick benchmark with 1K rows (for testing)
benchmark: build
	@echo "$(YELLOW)▶ Quick benchmark with 1K rows...$(RESET)"
	@mkdir -p ../data
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
	@mkdir -p ../data
	@if [ -f ../data/measurements-10m.txt ]; then \
		echo "$(GREEN)✓ Data file already exists:$(RESET) ../data/measurements-10m.txt"; \
		ls -lh ../data/measurements-10m.txt | awk '{print "  Size: " $$5}'; \
	else \
		echo "$(BLUE)▶ Generating 10M rows...$(RESET)"; \
		python $(GENERATOR) 10000000; \
		echo "$(GREEN)✓ Data generation complete!$(RESET)"; \
	fi

generate-medium:
	@mkdir -p ../data
	@if [ -f ../data/measurements-100m.txt ]; then \
		echo "$(GREEN)✓ Data file already exists:$(RESET) ../data/measurements-100m.txt"; \
		ls -lh ../data/measurements-100m.txt | awk '{print "  Size: " $$5}'; \
	else \
		echo "$(BLUE)▶ Generating 100M rows...$(RESET)"; \
		python $(GENERATOR) 100000000; \
		echo "$(GREEN)✓ Data generation complete!$(RESET)"; \
	fi

generate-large:
	@mkdir -p ../data
	@if [ -f ../data/measurements-500m.txt ]; then \
		echo "$(GREEN)✓ Data file already exists:$(RESET) ../data/measurements-500m.txt"; \
		ls -lh ../data/measurements-500m.txt | awk '{print "  Size: " $$5}'; \
	else \
		echo "$(BLUE)▶ Generating 500M rows...$(RESET)"; \
		python $(GENERATOR) 500000000; \
		echo "$(GREEN)✓ Data generation complete!$(RESET)"; \
	fi

generate-billion:
	@mkdir -p ../data
	@if [ -f ../data/measurements-1b.txt ]; then \
		echo "$(GREEN)✓ Data file already exists:$(RESET) ../data/measurements-1b.txt"; \
		ls -lh ../data/measurements-1b.txt | awk '{print "  Size: " $$5}'; \
	else \
		echo "$(MAGENTA)▶ Generating 1 BILLION rows $(YELLOW)(this will take a while!)$(RESET)"; \
		python $(GENERATOR) b; \
		echo "$(GREEN)✓ Data generation complete!$(RESET)"; \
	fi



# Profiling targets
profile: build
	@echo ""
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo "$(MAGENTA)$(BOLD)  Running with CPU + Memory Profiling$(RESET)"
	@echo "$(MAGENTA)$(BOLD)═══════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@./$(BINARY).exe -cpuprofile=$(CPU_PROFILE) -memprofile=$(MEM_PROFILE)
	@echo ""
	@echo "$(GREEN)✓ Profiling complete!$(RESET)"
	@echo "$(CYAN)  CPU profile:$(RESET) $(CPU_PROFILE)"
	@echo "$(CYAN)  Memory profile:$(RESET) $(MEM_PROFILE)"
	@echo ""
	@echo "$(YELLOW)Analyze with:$(RESET)"
	@echo "  $(BLUE)make pprof-cpu$(RESET)  - Analyze CPU usage"
	@echo "  $(BLUE)make pprof-mem$(RESET)  - Analyze memory allocations"
	@echo "  $(BLUE)make pprof-web$(RESET)  - Open in web browser"
	@echo ""

profile-cpu: build
	@echo "$(MAGENTA)▶ Running CPU profiling...$(RESET)"
	@./$(BINARY).exe -cpuprofile=$(CPU_PROFILE)
	@echo "$(GREEN)✓ CPU profile saved:$(RESET) $(CPU_PROFILE)"
	@echo "$(YELLOW)Analyze with:$(RESET) make pprof-cpu"

profile-mem: build
	@echo "$(MAGENTA)▶ Running memory profiling...$(RESET)"
	@./$(BINARY).exe -memprofile=$(MEM_PROFILE)
	@echo "$(GREEN)✓ Memory profile saved:$(RESET) $(MEM_PROFILE)"
	@echo "$(YELLOW)Analyze with:$(RESET) make pprof-mem

# Profile analysis targets
pprof-cpu:
	@if [ ! -f $(CPU_PROFILE) ]; then \
		echo "$(RED)Error: $(CPU_PROFILE) not found. Run 'make profile-cpu' first.$(RESET)"; \
		exit 1; \
	fi
	@echo "$(CYAN)Opening CPU profile in interactive mode...$(RESET)"
	@echo "$(YELLOW)Tip: Try 'top', 'list', 'web', or 'quit'$(RESET)"
	@go tool pprof $(BINARY).exe $(CPU_PROFILE)

pprof-mem:
	@if [ ! -f $(MEM_PROFILE) ]; then \
		echo "$(RED)Error: $(MEM_PROFILE) not found. Run 'make profile-mem' first.$(RESET)"; \
		exit 1; \
	fi
	@echo "$(CYAN)Opening memory profile in interactive mode...$(RESET)"
	@echo "$(YELLOW)Tip: Try 'top', 'list', 'web', or 'quit'$(RESET)"
	@go tool pprof $(BINARY).exe $(MEM_PROFILE)

pprof-web:
	@if [ ! -f $(CPU_PROFILE) ] && [ ! -f $(MEM_PROFILE) ]; then \
		echo "$(RED)Error: No profile files found. Run 'make profile' first.$(RESET)"; \
		exit 1; \
	fi
	@if [ -f $(CPU_PROFILE) ]; then \
		echo "$(CYAN)Opening CPU profile in web browser...$(RESET)"; \
		go tool pprof -http=:8080 $(BINARY).exe $(CPU_PROFILE); \
	elif [ -f $(MEM_PROFILE) ]; then \
		echo "$(CYAN)Opening memory profile in web browser...$(RESET)"; \
		go tool pprof -http=:8080 $(BINARY).exe $(MEM_PROFILE); \
	fi

# Clean generated files and binary
clean:
	@echo "$(RED)▶ Cleaning up...$(RESET)"
	@rm -f $(BINARY).exe
	@rm -f ../data/measurements-*.txt
	@echo "$(GREEN)✓ Clean complete!$(RESET)"

# Clean profiling files
clean-profiles:
	@echo "$(RED)▶ Removing profiling data...$(RESET)"
	@rm -f $(CPU_PROFILE) $(MEM_PROFILE)
	@rm -f *.prof
	@echo "$(GREEN)✓ Profiling data removed!$(RESET)"

# Clean everything including data directory
clean-all:
	@echo "$(RED)▶ Removing all generated files and directories...$(RESET)"
	@rm -f $(BINARY).exe
	@rm -f $(CPU_PROFILE) $(MEM_PROFILE)
	@rm -f *.prof
	@rm -rf ../data
	@go clean -cache -testcache -modcache
	@echo "$(GREEN)✓ Deep clean complete!$(RESET)"
