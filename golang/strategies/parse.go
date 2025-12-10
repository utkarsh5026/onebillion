package strategies

import (
	"bytes"
	"fmt"
	"strings"
)

func parseLineBasic(line string) (string, int64, error) {
	parts := strings.Split(line, ";")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid line format")
	}

	name := strings.TrimSpace(parts[0])
	val, err := stringToInt(strings.TrimSpace(parts[1]))

	return name, val, err
}

func parseLineByte(line []byte) (name []byte, value int64, err error) {
	colonIndex := bytes.IndexByte(line, ';')
	if colonIndex == -1 {
		return nil, -1, fmt.Errorf("invalid line format")
	}

	name = line[:colonIndex]
	valueBytes := line[colonIndex+1:]

	value, err = byteToInt(valueBytes)
	return name, value, err
}

func parseLineAdvanced(line []byte) (name []byte, value int64, err error) {
	semiColIdx := -1
	for i := range line {
		if line[i] == ';' {
			semiColIdx = i
			break
		}
	}

	if semiColIdx == -1 {
		return nil, -1, fmt.Errorf("invalid line format")
	}

	name = line[:semiColIdx]
	valBytes := line[semiColIdx+1:]

	var val int64
	neg := false
	vIDx := 0

	if len(valBytes) > 0 && valBytes[0] == '-' {
		neg = true
		vIDx++
	}

	for ; vIDx < len(valBytes); vIDx++ {
		if valBytes[vIDx] == '.' {
			continue
		}
		val = val*10 + int64(valBytes[vIDx]-'0')
	}
	if neg {
		val = -val
	}

	return name, val, nil
}

func parseLineUltra(line []byte) (name []byte, value int64, err error) {
	semiColIdx := bytes.IndexByte(line, ';')
	if semiColIdx == -1 {
		return nil, -1, fmt.Errorf("invalid line format")
	}

	name = line[:semiColIdx]
	valBytes := line[semiColIdx+1:]

	var val int64
	neg := false
	vIDx := 0

	if len(valBytes) > 0 && valBytes[0] == '-' {
		neg = true
		vIDx++
	}

	for ; vIDx < len(valBytes); vIDx++ {
		if valBytes[vIDx] == '.' {
			continue
		}
		val = val*10 + int64(valBytes[vIDx]-'0')
	}
	if neg {
		val = -val
	}

	return name, val, nil
}

func byteToInt(b []byte) (int64, error) {
	var result int64
	for i := range b {
		if b[i] == '.' {
			continue
		}
		result = result*10 + int64(b[i]-'0')
	}
	return result, nil
}

func stringToInt(s string) (int64, error) {
	var result int64

	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			continue
		}
		result = result*10 + int64(s[i]-'0')
	}
	return result, nil
}
