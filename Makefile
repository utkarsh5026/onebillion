.PHONY: help generate generate-small generate-large generate-billion clean clean-all

# Default target
help:
	@echo "Available targets:"
	@echo "  make generate         - Generate 100M rows (default, ~1.4GB)"
	@echo "  make generate-small   - Generate 10M rows (~140MB)"
	@echo "  make generate-large   - Generate 100M rows (~1.4GB)"
	@echo "  make generate-billion - Generate 1B rows (~14GB, takes time!)"
	@echo "  make clean           - Remove generated data files"
	@echo "  make clean-all       - Remove data directory and all contents"

# Generate default dataset (100M rows)
generate: generate-large

# Generate small dataset (10M rows) - for testing
generate-small:
	@echo "Generating 10M rows..."
	python generate.py 10000000

# Generate large dataset (100M rows) - default 1BRC size
generate-large:
	@echo "Generating 100M rows..."
	python generate.py

# Generate billion row dataset - full 1BRC challenge
generate-billion:
	@echo "Generating 1 billion rows (this will take a while)..."
	python generate.py b

# Clean generated data files
clean:
	@echo "Removing generated data files..."
	@if exist data\measurements.txt del /f data\measurements.txt
	@echo "Clean complete!"

# Clean all data directory
clean-all:
	@echo "Removing data directory..."
	@if exist data rmdir /s /q data
	@echo "Clean complete!"
