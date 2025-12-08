package main

import (
	"fmt"
	"onebillion/strategies"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

// BenchmarkResult holds the performance metrics for a strategy
type BenchmarkResult struct {
	StrategyName  string
	ExecutionTime time.Duration
	MemoryUsed    uint64
	ResultCount   int
	Success       bool
	Error         error
}

// ANSI color codes for terminal output
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
	ColorCyan   = "\033[36m"
	ColorWhite  = "\033[37m"
	ColorBold   = "\033[1m"
)

func main() {
	fmt.Printf("%s%s=== One Billion Row Challenge - Benchmark ===%s\n\n", ColorBold, ColorCyan, ColorReset)

	dataFile := getDataFile()

	strategies := []struct {
		name     string
		strategy strategies.Strategy
	}{
		{"Basic Strategy", &strategies.BasicStrategy{}},
	}

	results := make([]BenchmarkResult, 0, len(strategies))

	// Run each strategy and collect results
	for _, s := range strategies {
		fmt.Printf("%s⏱️  Running: %s%s\n", ColorYellow, s.name, ColorReset)
		result := benchmarkStrategy(s.name, s.strategy, dataFile)
		results = append(results, result)

		if result.Success {
			fmt.Printf("%s✓ Completed in: %v%s\n\n", ColorGreen, result.ExecutionTime, ColorReset)
		} else {
			fmt.Printf("%s✗ Failed: %v%s\n\n", ColorRed, result.Error, ColorReset)
		}
	}

	// Print summary
	printSummary(results)
}

func benchmarkStrategy(name string, strategy strategies.Strategy, filePath string) BenchmarkResult {
	result := BenchmarkResult{
		StrategyName: name,
		Success:      false,
	}

	runtime.GC()

	var memStatsBefore runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	// Start timing
	startTime := time.Now()

	// Execute strategy
	stationResults, err := strategy.Calculate(filePath)

	// End timing
	executionTime := time.Since(startTime)

	// Get memory stats after
	var memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsAfter)

	// Calculate memory used (in MB)
	memoryUsed := memStatsAfter.Alloc - memStatsBefore.Alloc

	result.ExecutionTime = executionTime
	result.MemoryUsed = memoryUsed
	result.ResultCount = len(stationResults)

	if err != nil {
		result.Error = err
		result.Success = false
	} else {
		result.Success = true
	}

	return result
}

func printSummary(results []BenchmarkResult) {
	fmt.Printf("%s%s=== Performance Summary ===%s\n\n", ColorBold, ColorCyan, ColorReset)

	if len(results) == 0 {
		fmt.Println("No results to display")
		return
	}

	// Print header
	fmt.Printf("%-25s %-15s %-15s %-10s %s\n",
		"Strategy", "Time", "Memory (MB)", "Results", "Status")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────")

	// Find fastest for comparison
	var fastest *BenchmarkResult
	for i := range results {
		if results[i].Success && (fastest == nil || results[i].ExecutionTime < fastest.ExecutionTime) {
			fastest = &results[i]
		}
	}

	// Print each result
	for _, result := range results {
		memoryMB := float64(result.MemoryUsed) / 1024 / 1024

		timeStr := formatDuration(result.ExecutionTime)
		memStr := fmt.Sprintf("%.2f MB", memoryMB)
		statusStr := ""
		color := ColorWhite

		if result.Success {
			if fastest != nil && result.StrategyName == fastest.StrategyName {
				statusStr = "✓ FASTEST"
				color = ColorGreen
			} else {
				statusStr = "✓"
				color = ColorWhite
			}
		} else {
			statusStr = "✗ FAILED"
			color = ColorRed
		}

		fmt.Printf("%s%-25s %-15s %-15s %-10d %s%s\n",
			color,
			result.StrategyName,
			timeStr,
			memStr,
			result.ResultCount,
			statusStr,
			ColorReset)

		if result.Error != nil {
			fmt.Printf("%s   Error: %v%s\n", ColorRed, result.Error, ColorReset)
		}
	}

	// Print comparison if multiple successful results
	successfulResults := 0
	for _, r := range results {
		if r.Success {
			successfulResults++
		}
	}

	if successfulResults > 1 && fastest != nil {
		fmt.Println()
		fmt.Printf("%s%sSpeed Comparison (relative to fastest):%s\n", ColorBold, ColorCyan, ColorReset)
		for _, result := range results {
			if result.Success && result.StrategyName != fastest.StrategyName {
				ratio := float64(result.ExecutionTime) / float64(fastest.ExecutionTime)
				fmt.Printf("  %s is %.2fx slower than %s\n",
					result.StrategyName, ratio, fastest.StrategyName)
			}
		}
	}
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.2f μs", float64(d.Microseconds()))
	}
	if d < time.Second {
		return fmt.Sprintf("%.2f ms", float64(d.Milliseconds()))
	}
	if d < time.Minute {
		return fmt.Sprintf("%.2f s", d.Seconds())
	}
	return fmt.Sprintf("%.2f min", d.Minutes())
}

// getDataFile determines which data file to use
// Priority: 1) Command line argument, 2) Most recent measurements-*.txt, 3) Default measurements.txt
func getDataFile() string {
	if len(os.Args) > 1 {
		dataFile := os.Args[1]
		if _, err := os.Stat(dataFile); err == nil {
			fmt.Printf("%sUsing data file:%s %s\n\n", ColorBlue, ColorReset, dataFile)
			return dataFile
		}
		fmt.Printf("%sWarning: File '%s' not found, searching for alternatives...%s\n", ColorYellow, dataFile, ColorReset)
	}

	dataDir := "data"
	pattern := filepath.Join(dataDir, "measurements-*.txt")
	matches, err := filepath.Glob(pattern)

	if err == nil && len(matches) > 0 {
		// Sort by modification time (most recent first)
		sort.Slice(matches, func(i, j int) bool {
			infoI, errI := os.Stat(matches[i])
			infoJ, errJ := os.Stat(matches[j])
			if errI != nil || errJ != nil {
				return false
			}
			return infoI.ModTime().After(infoJ.ModTime())
		})

		dataFile := matches[0]
		fileInfo, _ := os.Stat(dataFile)
		sizeMB := float64(fileInfo.Size()) / 1024 / 1024
		fmt.Printf("%sAuto-detected data file:%s %s %s(%.2f MB)%s\n\n",
			ColorBlue, ColorReset, dataFile, ColorYellow, sizeMB, ColorReset)
		return dataFile
	}

	defaultFile := filepath.Join(dataDir, "measurements.txt")
	fmt.Printf("%sUsing default data file:%s %s\n\n", ColorBlue, ColorReset, defaultFile)
	return defaultFile
}
