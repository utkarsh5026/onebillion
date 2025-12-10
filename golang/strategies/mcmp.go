package strategies

import (
	"bufio"
	"bytes"
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

	fsize, err := getFileSize(f)
	if err != nil {
		return nil, err
	}
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

	shouldSkipFirstLine, err := shouldSkipFirstLine(start, f)
	if err != nil {
		return err
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

type StationTableItem struct {
	Name                         []byte
	Hash                         uint32
	Sum, Count, Maximum, Minimum int64
	Occupied                     bool
}

const (
	tableSize = 131072
	tableMask = tableSize - 1
)

type MCMPLinearProbing struct{}

func (m *MCMPLinearProbing) Calculate(filePath string) ([]StationResult, error) {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fSize, err := getFileSize(f)
	if err != nil {
		return nil, err
	}
	_ = fSize

	n := runtime.NumCPU()
	chunkSize := fSize / int64(n)
	smaps := make([]StationMap, n)

	for i := range n {
		smaps[i] = make(StationMap, 100000)
	}

	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		start := int64(i) * chunkSize
		end := min(start+chunkSize, fSize)

		go func(start, end int64, smap StationMap) {
			defer wg.Done()
			m.processChunkLP(start, end, filePath, 64*1024, smap)
		}(start, end, smaps[i])
	}

	wg.Wait()
	mergedMap := mergeMaps(smaps)
	return calcAverges(mergedMap), nil
}

func (m *MCMPLinearProbing) processChunkLP(start, end int64, filePath string, bufferSize int, smap StationMap) error {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	items := make([]StationTableItem, tableSize)
	occupiedIndexes := make([]int, 0, 10000)

	reader := bufio.NewReaderSize(f, bufferSize)
	skipFirst, err := shouldSkipFirstLine(start, f)
	if err != nil {
		return err
	}

	_, err = f.Seek(start, 0)
	if err != nil {
		return err
	}

	currentPos := start

	if skipFirst {
		skipped, _ := reader.ReadBytes('\n')
		currentPos += int64(len(skipped))
	}

	for {
		if currentPos >= end {
			break
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}

		currentPos += int64(len(line))
		name, val, err := parseLineByte(line)

		if err != nil {
			return err
		}

		occ, idx := linearProbe(items, name, int64(val))
		if occ {
			occupiedIndexes = append(occupiedIndexes, idx)
		}
	}

	createStationMap(items, occupiedIndexes, smap)
	return nil
}

type MCMPLinearProbingOptimized struct{}

func (m *MCMPLinearProbingOptimized) Calculate(filePath string) ([]StationResult, error) {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fsize, err := getFileSize(f)
	if err != nil {
		return nil, err
	}
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
			m.processChunk(start, end, filePath, fileMap)
		}(start, end, tempMaps[i])
	}

	wg.Wait()
	return calcAverges(mergeMaps(tempMaps)), nil
}

func (m *MCMPLinearProbingOptimized) processChunk(start, end int64, filePath string, fileMap StationMap) error {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// --- FIX 2: Remove bufio. Handle skipping manually with f.Read ---
	if start > 0 {
		_, err = f.Seek(start-1, 0)
		if err != nil {
			return err
		}

		// Check if we are already at a newline
		tempBuf := make([]byte, 1)
		_, err = f.Read(tempBuf)
		if err != nil {
			return err
		}

		if tempBuf[0] != '\n' {
			// Read byte-by-byte until we find the start of the next line
			// (Optimization: You could read a small chunk here, but this runs once per core)
			b := make([]byte, 1)
			for {
				_, err := f.Read(b)
				if err != nil {
					return err
				}
				start++ // Keep track of how much we advanced
				if b[0] == '\n' {
					break
				}
			}
		}
	}

	// Seek to the exact start position
	_, err = f.Seek(start, 0)
	if err != nil {
		return err
	}

	return m.read(1024*1024, start, end, f, fileMap)
}

func (m *MCMPLinearProbingOptimized) read(bufferSize int, start, end int64, f *os.File, smap StationMap) error {
	items := make([]StationTableItem, tableSize)
	occupiedIndexes := make([]int, 0, 10000)

	buf := make([]byte, bufferSize)
	var leftover []byte

	for {
		if start >= end {
			break
		}

		n, err := f.Read(buf)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		filledBuf := buf[:n]
		if len(leftover) > 0 {
			filledBuf = append(leftover, filledBuf...)
			leftover = leftover[:0]
		}

		buffIdx := 0

		for {
			if buffIdx >= len(filledBuf) {
				break
			}

			lineEndIdx := bytes.IndexByte(filledBuf[buffIdx:], '\n')
			if lineEndIdx == -1 {
				leftover = append(leftover, filledBuf[buffIdx:]...)
				break
			}

			line := filledBuf[buffIdx : buffIdx+lineEndIdx]
			buffIdx += lineEndIdx + 1

			name, value, err := parseLineAdvanced(line)
			if err != nil {
				continue
			}

			occ, idx := linearProbe(items, name, int64(value))
			if occ {
				occupiedIndexes = append(occupiedIndexes, idx)
			}

		}
		start += int64(n)
	}
	createStationMap(items, occupiedIndexes, smap)
	return nil
}

// checks if we need to skip the first line in the chunk
// this is for a edge case where we start at the begining of a line
func shouldSkipFirstLine(start int64, f *os.File) (bool, error) {
	if start == 0 {
		return false, nil
	}

	_, err := f.Seek(start-1, 0)
	if err != nil {
		return false, err
	}

	buf := make([]byte, 1)
	_, err = f.Read(buf)
	if err != nil {
		return false, err
	}

	return buf[0] != '\n', nil
}

func linearProbe(items []StationTableItem, name []byte, value int64) (newOcc bool, occIndex int) {
	hash := hashFnv(name)
	index := hash & tableMask

	for {
		if !items[index].Occupied {
			items[index] = StationTableItem{
				Name:     name,
				Hash:     hash,
				Sum:      int64(value),
				Count:    1,
				Maximum:  value,
				Minimum:  value,
				Occupied: true,
			}
			newOcc = true
			break
		}
		if bytes.Equal(items[index].Name, name) {
			if value > items[index].Maximum {
				items[index].Maximum = value
			}
			if value < items[index].Minimum {
				items[index].Minimum = value
			}

			items[index].Sum += int64(value)
			items[index].Count++
			break
		}

		index = (index + 1) & tableMask
	}

	return newOcc, int(index)
}

func createStationMap(items []StationTableItem, occupiedIndexes []int, smap StationMap) {
	for _, idx := range occupiedIndexes {
		it := items[idx]
		smap[it.Hash] = StationResult{
			StationID: string(it.Name),
			Sum:       it.Sum,
			Count:     it.Count,
			Maximum:   it.Maximum,
			Minimum:   it.Minimum,
		}
	}
}
