package main

import (
	"bufio"
	"errors"
	"os"
	"strings"
)

// loadEnvFile reads simple KEY=VALUE lines from the provided file path and
// exports them to the current process if they are not already present.
func loadEnvFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		sep := strings.Index(line, "=")
		if sep <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:sep])
		if key == "" {
			continue
		}
		value := strings.TrimSpace(line[sep+1:])
		if isQuoted(value) {
			value = value[1 : len(value)-1]
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}

	return scanner.Err()
}

func isQuoted(value string) bool {
	if len(value) < 2 {
		return false
	}
	first := value[0]
	last := value[len(value)-1]
	if first != last {
		return false
	}
	return first == '"' || first == '\''
}
