package strategies

import (
	"bufio"
	"io"
	"os"
	"runtime"
	"sync"
)

type MCMPStrategy struct{}

func (m *MCMPStrategy) Calculate(filePath string) ([]StationResult, error) {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}

	fsize := fileInfo.Size()
	n := runtime.NumCPU()
	chunkSize := fsize / int64(n)
	tempMaps := make([]StationMap, n)

	for i := range n {
		tempMaps[i] = make(StationMap, 100000)
	}

	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		start := int64(i) * chunkSize
		end := min(start+chunkSize, fsize)
		go func(start, end int64, fileMap StationMap) {
			defer wg.Done()
			m.processChunk(start, end, filePath, 64*1024, fileMap)
		}(start, end, tempMaps[i])
	}

	wg.Wait()

	return calcAverges(mergeMaps(tempMaps)), nil
}

func (m *MCMPStrategy) processChunk(start, end int64, filePath string, bufferSize int, fileMap StationMap) error {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	shouldSkipFirstLine := false
	if start > 0 {
		_, err := f.Seek(start-1, 0)
		if err != nil {
			return err
		}

		buf := make([]byte, 1)
		_, err = f.Read(buf)

		if err != nil {
			return err
		}
		shouldSkipFirstLine = buf[0] != '\n'
	}

	_, err = f.Seek(start, 0)
	if err != nil {
		return err
	}

	reader := bufio.NewReaderSize(f, bufferSize)
	currentPos := start

	if shouldSkipFirstLine {
		skipped, _ := reader.ReadBytes('\n')
		currentPos += int64(len(skipped))
	}

	count := 0
	results := make([]Station, 0, 1024)
	for {
		if currentPos >= end {
			break
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		currentPos += int64(len(line))
		name, value, err := parseLineByte(line)
		if err != nil {
			continue
		}
		results = append(results, Station{Station: name, Value: value})
		count++

		if count >= 1024 {
			processBatch(results, fileMap)
			results = results[:0]
			count = 0
		}

		if err == io.EOF {
			break
		}
	}
	return nil
}
