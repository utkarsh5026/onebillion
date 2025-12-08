package strategies

import (
	"bufio"
	"math"
	"os"
)

type StationResult struct {
	StationID string
	Maximum   float64
	Minimum   float64
	Sum       int64
	Average   float64
	Count     int64
}

type Strategy interface {
	Calculate(filePath string) ([]StationResult, error)
}

type BasicStrategy struct{}

func (bs *BasicStrategy) Calculate(filePath string) ([]StationResult, error) {
	file, _ := os.Open(filePath)
	defer file.Close()

	stationMap := make(map[string]StationResult)

	scnanner := bufio.NewScanner(file)
	for scnanner.Scan() {
		line := scnanner.Text()
		name, value, err := parseLineBasic(line)
		if err != nil {
			return nil, err
		}

		if _, exists := stationMap[name]; !exists {
			stationMap[name] = StationResult{
				StationID: name,
				Maximum:   math.Inf(-1),
				Minimum:   math.Inf(1),
				Sum:       int64(value),
			}
		}

		res := stationMap[name]
		if value > res.Maximum {
			res.Maximum = value
		}

		if value < res.Minimum {
			res.Minimum = value
		}

		res.Sum += int64(value)
		res.Count++
		stationMap[name] = res
	}

	return calcAverges(stationMap), nil
}

func calcAverges(stationMap map[string]StationResult) []StationResult {
	results := make([]StationResult, 0, len(stationMap))

	for _, res := range stationMap {
		res.Average = float64(res.Sum) / 24.0
		results = append(results, res)
	}
	return results
}
