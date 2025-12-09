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
	Name                         string
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
	occupiedIndexes := make([]int, 0, tableSize)

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

		hash := hashFnv(name)
		index := hash & tableMask

		for {
			if !items[index].Occupied {
				items[index] = StationTableItem{
					Name:     string(name),
					Sum:      val,
					Count:    1,
					Maximum:  val,
					Minimum:  val,
					Occupied: true,
				}
				occupiedIndexes = append(occupiedIndexes, int(index))
				break
			}

			if items[index].Name == string(name) {
				if val > items[index].Maximum {
					items[index].Maximum = val
				}
				if val < items[index].Minimum {
					items[index].Minimum = val
				}
				items[index].Sum += val
				items[index].Count++
				break
			}

			index = (index + 1) & tableMask
		}
	}

	for _, idx := range occupiedIndexes {
		it := items[idx]
		h := hashFnv([]byte(it.Name))
		smap[h] = StationResult{
			StationID: it.Name,
			Sum:       it.Sum,
			Count:     it.Count,
			Maximum:   it.Maximum,
			Minimum:   it.Minimum,
		}
	}

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
