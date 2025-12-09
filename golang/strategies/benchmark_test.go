package strategies

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
)

// Common city names for test data generation
var testCities = []string{
	"Hamburg", "Berlin", "Tokyo", "Sydney", "New York", "London", "Paris", "Moscow",
	"Beijing", "Mumbai", "Cairo", "Rio", "Toronto", "Dubai", "Singapore", "Stockholm",
	"Oslo", "Helsinki", "Warsaw", "Prague", "Vienna", "Rome", "Madrid", "Lisbon",
	"Athens", "Istanbul", "Bangkok", "Seoul", "Manila", "Jakarta", "Delhi", "Shanghai",
}

// generateTempTestData creates a temporary test file with specified number of measurements
func generateTempTestData(b *testing.B, numRows int) string {
	tmpFile, err := os.CreateTemp("", "measurements-*.txt")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	defer tmpFile.Close()

	// Ensure cleanup after benchmark
	b.Cleanup(func() {
		os.Remove(tmpFile.Name())
	})

	// Generate random measurements
	for i := 0; i < numRows; i++ {
		city := testCities[rand.Intn(len(testCities))]
		// Temperature range: -50.0 to 50.0
		temp := (rand.Float64() * 100.0) - 50.0
		line := fmt.Sprintf("%s;%.1f\n", city, temp)
		if _, err := tmpFile.WriteString(line); err != nil {
			b.Fatalf("Failed to write to temp file: %v", err)
		}
	}

	return tmpFile.Name()
}

// getTestDataFile generates a temp test file for benchmarking
// Default: 100,000 rows (~2MB) - fast enough for quick benchmarks
func getTestDataFile(b *testing.B) string {
	return generateTempTestData(b, 100_000)
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

// BenchmarkMCMPStrategy benchmarks the multi-core multi-processing strategy
func BenchmarkMCMPStrategy(b *testing.B) {
	dataFile := getTestDataFile(b)
	strategy := &MCMPStrategy{}

	for b.Loop() {
		_, err := strategy.Calculate(dataFile)
		if err != nil {
			b.Fatalf("MCMPStrategy failed: %v", err)
		}
	}
}

// BenchmarkMCMPStrategyMemory benchmarks memory allocation for MCMP strategy
func BenchmarkMCMPStrategyMemory(b *testing.B) {
	dataFile := getTestDataFile(b)
	strategy := &MCMPStrategy{}

	b.ReportAllocs()

	for b.Loop() {
		_, err := strategy.Calculate(dataFile)
		if err != nil {
			b.Fatalf("MCMPStrategy failed: %v", err)
		}
	}
}

// BenchmarkMCMPStrategyWithCPUs benchmarks the MCMP strategy with varying CPU counts
func BenchmarkMCMPStrategyWithCPUs(b *testing.B) {
	dataFile := getTestDataFile(b)

	cpuCounts := []int{1, 2, 4, 8, 16}
	originalCPU := runtime.GOMAXPROCS(0)

	for _, numCPU := range cpuCounts {
		if numCPU > runtime.NumCPU() {
			continue
		}

		b.Run(formatCPUCount(numCPU), func(b *testing.B) {
			runtime.GOMAXPROCS(numCPU)
			strategy := &MCMPStrategy{}

			b.ResetTimer()
			for b.Loop() {
				_, err := strategy.Calculate(dataFile)
				if err != nil {
					b.Fatalf("MCMPStrategy failed: %v", err)
				}
			}
		})
	}

	runtime.GOMAXPROCS(originalCPU)
}
