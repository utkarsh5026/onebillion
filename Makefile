.PHONY: help generate generate-small generate-large generate-billion clean clean-all

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
	@echo "  $(GREEN)make generate-small$(RESET)   - Generate 10M rows $(YELLOW)(~140MB, for testing)$(RESET)"
	@echo "  $(GREEN)make generate-large$(RESET)   - Generate 100M rows $(YELLOW)(~1.4GB, standard 1BRC)$(RESET)"
	@echo "  $(GREEN)make generate-billion$(RESET) - Generate 1B rows $(YELLOW)(~14GB, full challenge!)$(RESET)"
	@echo "  $(RED)make clean$(RESET)            - Remove all generated data files"
	@echo "  $(RED)make clean-all$(RESET)        - Remove data directory and all contents"
	@echo ""

# Generate default dataset (100M rows)
generate: generate-large

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
