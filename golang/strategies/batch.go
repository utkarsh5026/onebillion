package strategies

import (
	"bufio"
	"os"
	"runtime"
	"sync"
)

type BatchStrategy struct{}

func (b *BatchStrategy) Calculate(filePath string) ([]StationResult, error) {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	n := runtime.NumCPU()
	resChan := make(chan []Station, n)
	finalBatch := make([]map[uint32]StationResult, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			temp := make(map[uint32]StationResult, 1000)
			for r := range resChan {
				processBatch(r, temp)
			}
			finalBatch[i] = temp
		}(i)
	}

	batchSize := 100
	batch := make([]Station, 0, batchSize)
	for scanner.Scan() {
		line := scanner.Bytes()
		nameBytes, value, err := parseLineByte(line)
		if err != nil {
			return nil, err
		}

		batch = append(batch, Station{Station: nameBytes, Value: value})
		if len(batch) >= batchSize {
			resChan <- batch
			batch = make([]Station, 0, batchSize)
		}
	}

	close(resChan)
	wg.Wait()
	return calcAverges(b.merge(finalBatch)), nil
}

func (b *BatchStrategy) merge(maps []StationMap) StationMap {
	keyCount := 0
	for _, m := range maps {
		keyCount += len(m)
	}

	merged := make(StationMap, keyCount)
	for _, m := range maps {
		for hash, res := range m {
			if existing, exists := merged[hash]; exists {
				if res.Maximum > existing.Maximum {
					existing.Maximum = res.Maximum
				}

				if res.Minimum < existing.Minimum {
					existing.Minimum = res.Minimum
				}

				existing.Sum += res.Sum
				existing.Count += res.Count
				merged[hash] = existing
			} else {
				merged[hash] = res
			}
		}
	}
	return merged
}
