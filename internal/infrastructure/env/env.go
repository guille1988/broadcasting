package env

import (
	"os"
	"strconv"
)

// GetEnvAsString returns the value of the given environment variable as a string.
func GetEnvAsString(key string, fallback string) string {
	value, found := os.LookupEnv(key)

	if !found {
		return fallback
	}

	return value
}

// GetEnvAsInt returns the value of the given environment variable as an integer.
func GetEnvAsInt(key string, fallback int) int {
	stringValue := GetEnvAsString(key, "")
	value, err := strconv.Atoi(stringValue)

	if err == nil {
		return value
	}

	return fallback
}
