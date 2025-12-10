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

// strategyBenchmark holds a strategy and its name for benchmarking
type strategyBenchmark struct {
	name     string
	strategy Strategy
}

// getAllStrategies returns all strategies to benchmark
func getAllStrategies() []strategyBenchmark {
	return []strategyBenchmark{
		{"Basic", &BasicStrategy{}},
		{"ByteReading", &ByteReadingStrategy{}},
		{"Batch", &BatchStrategy{}},
		{"MCMP", &MCMPStrategy{}},
	}
}

// BenchmarkAllStrategies benchmarks all strategies
func BenchmarkAllStrategies(b *testing.B) {
	dataFile := getTestDataFile(b)
	strategies := getAllStrategies()

	for _, s := range strategies {
		b.Run(s.name, func(b *testing.B) {
			for b.Loop() {
				_, err := s.strategy.Calculate(dataFile)
				if err != nil {
					b.Fatalf("%s failed: %v", s.name, err)
				}
			}
		})
	}
}

// BenchmarkParseLineFunctions compares all three parsing functions
func BenchmarkParseLineFunctions(b *testing.B) {
	testLineString := "Hamburg;12.0"
	testLineBytes := []byte("Hamburg;12.0")

	b.Run("Basic", func(b *testing.B) {
		for b.Loop() {
			_, _, err := parseLineBasic(testLineString)
			if err != nil {
				b.Fatalf("parseLineBasic failed: %v", err)
			}
		}
	})

	b.Run("Byte", func(b *testing.B) {
		for b.Loop() {
			_, _, err := parseLineByte(testLineBytes)
			if err != nil {
				b.Fatalf("parseLineByte failed: %v", err)
			}
		}
	})

	b.Run("Advanced", func(b *testing.B) {
		for b.Loop() {
			_, _, err := parseLineAdvanced(testLineBytes)
			if err != nil {
				b.Fatalf("parseLineAdvanced failed: %v", err)
			}
		}
	})

	b.Run("Ultra", func(b *testing.B) {
		for b.Loop() {
			_, _, err := parseLineUltra(testLineBytes)
			if err != nil {
				b.Fatalf("parseLineUltra failed: %v", err)
			}
		}
	})
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

// BenchmarkAllStrategiesMemory tests memory allocation patterns for all strategies
func BenchmarkAllStrategiesMemory(b *testing.B) {
	dataFile := getTestDataFile(b)
	strategies := getAllStrategies()

	for _, s := range strategies {
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				_, err := s.strategy.Calculate(dataFile)
				if err != nil {
					b.Fatalf("%s failed: %v", s.name, err)
				}
			}
		})
	}
}

func formatCPUCount(n int) string {
	if n == 1 {
		return "1CPU"
	}
	return string(rune('0'+n)) + "CPUs"
}

// BenchmarkAllStrategiesWithCPUs benchmarks all strategies with varying CPU counts
func BenchmarkAllStrategiesWithCPUs(b *testing.B) {
	dataFile := getTestDataFile(b)
	strategies := getAllStrategies()

	cpuCounts := []int{1, 2, 4, 8, 16}
	originalCPU := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(originalCPU)

	for _, s := range strategies {
		b.Run(s.name, func(b *testing.B) {
			for _, numCPU := range cpuCounts {
				if numCPU > runtime.NumCPU() {
					continue
				}

				b.Run(formatCPUCount(numCPU), func(b *testing.B) {
					runtime.GOMAXPROCS(numCPU)

					b.ResetTimer()
					for b.Loop() {
						_, err := s.strategy.Calculate(dataFile)
						if err != nil {
							b.Fatalf("%s failed: %v", s.name, err)
						}
					}
				})
			}
		})
	}
}
