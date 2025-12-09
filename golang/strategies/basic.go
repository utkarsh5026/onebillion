package strategies

import (
	"bufio"
	"math"
	"os"
)

type Strategy interface {
	Calculate(filePath string) ([]StationResult, error)
}

type StationResult struct {
	StationID                    string
	Maximum, Minimum, Sum, Count int64
	Average                      float64
}

func newSt(name string) StationResult {
	return StationResult{
		StationID: name,
		Maximum:   math.MinInt64,
		Minimum:   math.MaxInt64,
		Count:     0,
	}
}

type BasicStrategy struct{}

func (bs *BasicStrategy) Calculate(filePath string) ([]StationResult, error) {
	file, _ := os.Open(filePath)
	defer file.Close()

	stationMap := make(map[string]StationResult)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		scanner.Bytes()
		name, value, err := parseLineBasic(line)
		if err != nil {
			return nil, err
		}

		if _, exists := stationMap[name]; !exists {
			stationMap[name] = newSt(name)
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

func calcAverges[K comparable](stationMap map[K]StationResult) []StationResult {
	results := make([]StationResult, 0, len(stationMap))

	for _, res := range stationMap {
		res.Average = float64(res.Sum) / 24.0
		results = append(results, res)
	}
	return results
}

type ByteReadingStrategy struct{}

func (brs *ByteReadingStrategy) Calculate(filePath string) ([]StationResult, error) {
	file, _ := os.Open(filePath)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	stationMap := make(map[uint32]StationResult)

	for scanner.Scan() {
		line := scanner.Bytes()

		nameBytes, value, err := parseLineByte(line)
		if err != nil {
			return nil, err
		}

		hash := brs.hashFnv(nameBytes)
		name := string(nameBytes)

		if _, exists := stationMap[hash]; !exists {
			stationMap[hash] = newSt(name)
		}

		res := stationMap[hash]
		if value > res.Maximum {
			res.Maximum = value
		}
		if value < res.Minimum {
			res.Minimum = value
		}
		res.Sum += int64(value)
		res.Count++
		stationMap[hash] = res
	}

	return calcAverges(stationMap), nil
}

func (brs *ByteReadingStrategy) hashFnv(name []byte) uint32 {
	var hash uint32 = 2166136261
	const prime32 = 16777619

	for i := range name {
		hash ^= uint32(name[i])
		hash *= prime32
	}
	return hash
}
