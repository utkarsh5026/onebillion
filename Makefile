.PHONY: help generate generate-tiny generate-small generate-large generate-billion generate-all verify verify-tiny verify-small verify-large verify-billion verify-100k verify-1m verify-10m verify-100m verify-all clean clean-all

# Color codes (ANSI)
BLUE := \033[1;34m
GREEN := \033[1;32m
YELLOW := \033[1;33m
RED := \033[1;31m
CYAN := \033[1;36m
MAGENTA := \033[1;35m
BOLD := \033[1m
RESET := \033[0m

# Default target
help:
	@echo "$(CYAN)╔════════════════════════════════════════════════════════════════════════╗$(RESET)"
	@echo "$(CYAN)║$(RESET)  $(BOLD)One Billion Row Challenge - Data Generator$(RESET)$(CYAN)                            ║$(RESET)"
	@echo "$(CYAN)╚════════════════════════════════════════════════════════════════════════╝$(RESET)"
	@echo ""
	@echo "$(BOLD)Available targets:$(RESET)"
	@echo "  $(GREEN)make generate$(RESET)         - Generate 100M rows $(YELLOW)(default, ~1.4GB)$(RESET)"
	@echo "  $(GREEN)make generate-tiny$(RESET)    - Generate 1M rows $(YELLOW)(~14MB, quick testing)$(RESET)"
	@echo "  $(GREEN)make generate-small$(RESET)   - Generate 10M rows $(YELLOW)(~140MB, for testing)$(RESET)"
	@echo "  $(GREEN)make generate-large$(RESET)   - Generate 100M rows $(YELLOW)(~1.4GB, standard 1BRC)$(RESET)"
	@echo "  $(GREEN)make generate-billion$(RESET) - Generate 1B rows $(YELLOW)(~14GB, full challenge!)$(RESET)"
	@echo "  $(MAGENTA)make generate-all$(RESET)     - Generate all datasets $(YELLOW)(10M + 100M + 1B, ~15.5GB total)$(RESET)"
	@echo ""
	@echo "$(BOLD)Verification:$(RESET)"
	@echo "  $(CYAN)make verify$(RESET)           - Verify 100M dataset $(YELLOW)(default)$(RESET)"
	@echo "  $(CYAN)make verify-tiny$(RESET)      - Verify 1M dataset $(YELLOW)(~0.1s)$(RESET)"
	@echo "  $(CYAN)make verify-small$(RESET)     - Verify 10M dataset $(YELLOW)(~1s)$(RESET)"
	@echo "  $(CYAN)make verify-large$(RESET)     - Verify 100M dataset $(YELLOW)(~10s)$(RESET)"
	@echo "  $(CYAN)make verify-billion$(RESET)   - Verify 1B dataset $(YELLOW)(if available)$(RESET)"
	@echo "  $(MAGENTA)make verify-all$(RESET)       - Verify all available datasets"
	@echo ""
	@echo "$(BOLD)Cleanup:$(RESET)"
	@echo "  $(RED)make clean$(RESET)            - Remove all generated data files"
	@echo "  $(RED)make clean-all$(RESET)        - Remove data directory and all contents"
	@echo ""

# Generate default dataset (100M rows)
generate: generate-large

# Generate tiny dataset (1M rows) - for quick testing
generate-tiny:
	@echo "$(BLUE)▶ Generating 1M rows$(RESET) → $(CYAN)measurements-1m.txt$(RESET)"
	@python generate.py 1000000
	@echo "$(GREEN)✓ Generation complete!$(RESET)"

# Generate small dataset (10M rows) - for testing
generate-small:
	@echo "$(BLUE)▶ Generating 10M rows$(RESET) → $(CYAN)measurements-10m.txt$(RESET)"
	@python generate.py 10000000
	@echo "$(GREEN)✓ Generation complete!$(RESET)"

# Generate large dataset (100M rows) - default 1BRC size
generate-large:
	@echo "$(BLUE)▶ Generating 100M rows$(RESET) → $(CYAN)measurements-100m.txt$(RESET)"
	@python generate.py
	@echo "$(GREEN)✓ Generation complete!$(RESET)"

# Generate billion row dataset - full 1BRC challenge
generate-billion:
	@echo "$(MAGENTA)▶ Generating 1 BILLION rows$(RESET) → $(CYAN)measurements-1b.txt$(RESET) $(YELLOW)(this will take a while!)$(RESET)"
	@python generate.py b
	@echo "$(GREEN)✓ Generation complete!$(RESET)"

# Generate all datasets (small + large + billion)
generate-all: generate-small generate-large generate-billion
	@echo "$(MAGENTA)✓ All datasets generated successfully!$(RESET)"

# Verify default dataset (100M rows)
verify: verify-large

# Verify tiny dataset (1M rows)
verify-tiny:
	@echo "$(BLUE)▶ Verifying 1M rows$(RESET) → $(CYAN)results-1m.csv$(RESET)"
	@python verify.py 1m
	@echo "$(GREEN)✓ Verification complete!$(RESET)"

# Verify small dataset (10M rows)
verify-small:
	@echo "$(BLUE)▶ Verifying 10M rows$(RESET) → $(CYAN)results-10m.csv$(RESET)"
	@python verify.py 10m
	@echo "$(GREEN)✓ Verification complete!$(RESET)"

# Verify large dataset (100M rows) - default verification size
verify-large:
	@echo "$(BLUE)▶ Verifying 100M rows$(RESET) → $(CYAN)results-100m.csv$(RESET)"
	@python verify.py 100m
	@echo "$(GREEN)✓ Verification complete!$(RESET)"

# Verify billion row dataset - full challenge
verify-billion:
	@echo "$(MAGENTA)▶ Verifying 1 BILLION rows$(RESET) → $(CYAN)results-1b.csv$(RESET)"
	@python verify.py 1b
	@echo "$(GREEN)✓ Verification complete!$(RESET)"

# Verify all available datasets
verify-all:
	@echo "$(MAGENTA)▶ Verifying all available datasets...$(RESET)"
	@python verify.py --all
	@echo "$(MAGENTA)✓ All verifications complete!$(RESET)"

# Legacy verification targets (for specific sizes)
verify-100k:
	@echo "$(CYAN)▶ Verifying 100K dataset$(RESET)"
	@python verify.py 100k

verify-1m: verify-tiny

verify-10m: verify-small

verify-100m: verify-large

# Clean all generated data files
clean:
	@echo "$(RED)▶ Removing generated data files...$(RESET)"
	@if exist data\measurements-*.txt del /f data\measurements-*.txt
	@echo "$(GREEN)✓ Clean complete!$(RESET)"

# Clean all data directory
clean-all:
	@echo "$(RED)▶ Removing data directory...$(RESET)"
	@if exist data rmdir /s /q data
	@echo "$(GREEN)✓ Clean complete!$(RESET)"
