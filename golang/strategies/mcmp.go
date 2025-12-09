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
		hash := hashFnv(name)
		st, exists := fileMap[hash]
		if !exists {
			st = newSt(string(name))
		}

		st.Sum += int64(value)
		if value > st.Maximum {
			st.Maximum = value
		}
		if value < st.Minimum {
			st.Minimum = value
		}
		fileMap[hash] = st
		count++

		if err == io.EOF {
			break
		}
	}
	return nil
}
