package strategies

import (
	"fmt"
	"strings"
)

func parseLineBasic(line string) (string, float64, error) {
	parts := strings.Split(line, ";")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid line format")
	}

	name := strings.TrimSpace(parts[0])
	var value float64
	_, err := fmt.Sscanf(strings.TrimSpace(parts[1]), "%f", &value)
	if err != nil {
		return "", 0, fmt.Errorf("invalid value format")
	}

	return name, value, nil
}
