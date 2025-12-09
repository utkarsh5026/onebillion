package main

import (
	"flag"
	"fmt"
	"onebillion/strategies"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"text/tabwriter"
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

var (
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile = flag.String("memprofile", "", "write memory profile to file")
)

func main() {
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Printf("%sError creating CPU profile: %v%s\n", ColorRed, err, ColorReset)
			os.Exit(1)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Printf("%sError starting CPU profile: %v%s\n", ColorRed, err, ColorReset)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
		fmt.Printf("%s📊 CPU profiling enabled → %s%s\n", ColorGreen, *cpuprofile, ColorReset)
	}

	if *memprofile != "" {
		defer func() {
			f, err := os.Create(*memprofile)
			if err != nil {
				fmt.Printf("%sError creating memory profile: %v%s\n", ColorRed, err, ColorReset)
				return
			}
			defer f.Close()

			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				fmt.Printf("%sError writing memory profile: %v%s\n", ColorRed, err, ColorReset)
			} else {
				fmt.Printf("%s📊 Memory profile saved → %s%s\n", ColorGreen, *memprofile, ColorReset)
			}
		}()
	}

	fmt.Printf("%s%s=== One Billion Row Challenge - Benchmark ===%s\n\n", ColorBold, ColorCyan, ColorReset)

	dataFile := getDataFile()

	strategies := []struct {
		name     string
		strategy strategies.Strategy
	}{
		{"Batch Strategy", &strategies.BatchStrategy{}},
		{"Basic Strategy", &strategies.BasicStrategy{}},
		{"Byte Strategy", &strategies.ByteReadingStrategy{}},
	}

	results := make([]BenchmarkResult, 0, len(strategies))

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

	// Find the fastest strategy
	var fastest *BenchmarkResult
	for i := range results {
		if results[i].Success && (fastest == nil || results[i].ExecutionTime < fastest.ExecutionTime) {
			fastest = &results[i]
		}
	}

	// Create a tabwriter for nicely formatted table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	// Print header
	fmt.Fprintf(w, "%s%sSTRATEGY\tTIME\tMEMORY (MB)\tRESULTS\tSTATUS%s\n",
		ColorBold, ColorCyan, ColorReset)
	fmt.Fprintf(w, "───────────────────────\t────────────\t───────────\t────────\t──────────────\n")

	// Add rows to the table
	for _, result := range results {
		memoryMB := float64(result.MemoryUsed) / 1024 / 1024
		timeStr := formatDuration(result.ExecutionTime)
		statusStr := ""
		rowColor := ""

		if result.Success {
			if fastest != nil && result.StrategyName == fastest.StrategyName {
				statusStr = "✓ FASTEST"
				rowColor = ColorGreen
			} else {
				statusStr = "✓"
				rowColor = ""
			}
		} else {
			statusStr = "✗ FAILED"
			rowColor = ColorRed
		}

		fmt.Fprintf(w, "%s%s\t%s\t%.2f\t%d\t%s%s\n",
			rowColor,
			result.StrategyName,
			timeStr,
			memoryMB,
			result.ResultCount,
			statusStr,
			ColorReset)

		// Add error row if needed
		if result.Error != nil {
			fmt.Fprintf(w, "%s  Error: %v%s\t\t\t\t\n", ColorRed, result.Error, ColorReset)
		}
	}

	w.Flush()

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
	args := flag.Args()
	if len(args) > 0 {
		dataFile := args[0]
		if _, err := os.Stat(dataFile); err == nil {
			fmt.Printf("%sUsing data file:%s %s\n\n", ColorBlue, ColorReset, dataFile)
			return dataFile
		}
		fmt.Printf("%sWarning: File '%s' not found, searching for alternatives...%s\n", ColorYellow, dataFile, ColorReset)
	}

	dataDir := "../data"
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
