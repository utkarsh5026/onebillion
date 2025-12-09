package strategies

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// getTestDataFile locates the test data file
func getTestDataFile(b *testing.B) string {
	// Try different possible locations
	possiblePaths := []string{
		"../../data/measurements.txt",
		"../../data/measurements-100.txt",
		"../../data/measurements-1000.txt",
		"../data/measurements.txt",
		"data/measurements.txt",
	}

	for _, path := range possiblePaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	pattern := "../../data/measurements*.txt"
	matches, err := filepath.Glob(pattern)
	if err == nil && len(matches) > 0 {
		return matches[0]
	}

	b.Fatal("No test data file found. Please ensure a measurements*.txt file exists in the data directory")
	return ""
}

// BenchmarkBasicStrategy benchmarks the basic string-based strategy
func BenchmarkBasicStrategy(b *testing.B) {
	dataFile := getTestDataFile(b)
	strategy := &BasicStrategy{}

	for b.Loop() {
		_, err := strategy.Calculate(dataFile)
		if err != nil {
			b.Fatalf("BasicStrategy failed: %v", err)
		}
	}
}

// BenchmarkByteReadingStrategy benchmarks the byte-based strategy with hashing
func BenchmarkByteReadingStrategy(b *testing.B) {
	dataFile := getTestDataFile(b)
	strategy := &ByteReadingStrategy{}

	for b.Loop() {
		_, err := strategy.Calculate(dataFile)
		if err != nil {
			b.Fatalf("ByteReadingStrategy failed: %v", err)
		}
	}
}

// BenchmarkBatchStrategy benchmarks the concurrent batch processing strategy
func BenchmarkBatchStrategy(b *testing.B) {
	dataFile := getTestDataFile(b)
	strategy := &BatchStrategy{}

	for b.Loop() {
		_, err := strategy.Calculate(dataFile)
		if err != nil {
			b.Fatalf("BatchStrategy failed: %v", err)
		}
	}
}

// BenchmarkParseLineBasic benchmarks the basic line parsing function
func BenchmarkParseLineBasic(b *testing.B) {
	testLine := "Hamburg;12.0"

	for b.Loop() {
		_, _, err := parseLineBasic(testLine)
		if err != nil {
			b.Fatalf("parseLineBasic failed: %v", err)
		}
	}
}

// BenchmarkParseLineByte benchmarks the byte-based line parsing function
func BenchmarkParseLineByte(b *testing.B) {
	testLine := []byte("Hamburg;12.0")

	for b.Loop() {
		_, _, err := parseLineByte(testLine)
		if err != nil {
			b.Fatalf("parseLineByte failed: %v", err)
		}
	}
}

// BenchmarkHashFnv benchmarks the FNV hashing function
func BenchmarkHashFnv(b *testing.B) {
	testName := []byte("Hamburg")

	for b.Loop() {
		_ = hashFnv(testName)
	}
}

// BenchmarkStringToInt benchmarks string to integer conversion
func BenchmarkStringToInt(b *testing.B) {
	testString := "12.0"

	for b.Loop() {
		_, err := stringToInt(testString)
		if err != nil {
			b.Fatalf("stringToInt failed: %v", err)
		}
	}
}

// BenchmarkByteToInt benchmarks byte to integer conversion
func BenchmarkByteToInt(b *testing.B) {
	testBytes := []byte("12.0")

	for b.Loop() {
		_, err := byteToInt(testBytes)
		if err != nil {
			b.Fatalf("byteToInt failed: %v", err)
		}
	}
}

// BenchmarkMemory tests memory allocation patterns
func BenchmarkBasicStrategyMemory(b *testing.B) {
	dataFile := getTestDataFile(b)
	strategy := &BasicStrategy{}

	b.ReportAllocs()

	for b.Loop() {
		_, err := strategy.Calculate(dataFile)
		if err != nil {
			b.Fatalf("BasicStrategy failed: %v", err)
		}
	}
}

func BenchmarkByteReadingStrategyMemory(b *testing.B) {
	dataFile := getTestDataFile(b)
	strategy := &ByteReadingStrategy{}

	b.ReportAllocs()

	for b.Loop() {
		_, err := strategy.Calculate(dataFile)
		if err != nil {
			b.Fatalf("ByteReadingStrategy failed: %v", err)
		}
	}
}

func BenchmarkBatchStrategyMemory(b *testing.B) {
	dataFile := getTestDataFile(b)
	strategy := &BatchStrategy{}

	b.ReportAllocs()

	for b.Loop() {
		_, err := strategy.Calculate(dataFile)
		if err != nil {
			b.Fatalf("BatchStrategy failed: %v", err)
		}
	}
}

// BenchmarkBatchStrategyParallel benchmarks the batch strategy with varying CPU counts
func BenchmarkBatchStrategyWithCPUs(b *testing.B) {
	dataFile := getTestDataFile(b)

	cpuCounts := []int{1, 2, 4, 8}
	originalCPU := runtime.GOMAXPROCS(0)

	for _, numCPU := range cpuCounts {
		if numCPU > runtime.NumCPU() {
			continue
		}

		b.Run(formatCPUCount(numCPU), func(b *testing.B) {
			runtime.GOMAXPROCS(numCPU)
			strategy := &BatchStrategy{}

			b.ResetTimer()
			for b.Loop() {
				_, err := strategy.Calculate(dataFile)
				if err != nil {
					b.Fatalf("BatchStrategy failed: %v", err)
				}
			}
		})
	}

	runtime.GOMAXPROCS(originalCPU)
}

func formatCPUCount(n int) string {
	if n == 1 {
		return "1CPU"
	}
	return string(rune('0'+n)) + "CPUs"
}
