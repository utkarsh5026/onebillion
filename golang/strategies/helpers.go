package strategies

type StationMap = map[uint32]StationResult

type Station struct {
	Station []byte
	Value   int64
}

func processBatch(results []Station, stationMap map[uint32]StationResult) {
	for _, r := range results {
		hash := hashFnv(r.Station)
		name := string(r.Station)
		if _, exists := stationMap[hash]; !exists {
			stationMap[hash] = newSt(name)
		}

		res := stationMap[hash]
		if r.Value > res.Maximum {
			res.Maximum = r.Value
		}

		if r.Value < res.Minimum {
			res.Minimum = r.Value
		}

		res.Sum += int64(r.Value)
		res.Count++
		stationMap[hash] = res
	}
}

func hashFnv(name []byte) uint32 {
	var hash uint32 = 2166136261
	const prime32 = 16777619

	for i := range name {
		hash ^= uint32(name[i])
		hash *= prime32
	}
	return hash
}

func mergeMaps(maps []StationMap) StationMap {
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
