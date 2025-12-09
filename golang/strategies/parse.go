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
