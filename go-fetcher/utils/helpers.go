package utils

import (
	"fmt"
	"os"
	"strings"
)

// GetEnv retrieves environment variable or returns fallback
func GetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// EnvInt retrieves environment variable as integer or returns fallback
func EnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		_, _ = fmt.Sscanf(v, "%d", &n)
		if n > 0 {
			return n
		}
	}
	return fallback
}

// SameHost checks if two hosts are the same (case-insensitive)
func SameHost(a, b string) bool {
	return strings.EqualFold(a, b)
}

// Percent calculates percentage
func Percent(done, total int) float64 {
	if total <= 0 {
		return 0
	}
	return (float64(done) / float64(total)) * 100.0
}

// SanitizeFilename converts URL to a safe filename
func SanitizeFilename(urlStr string) string {
	urlStr = strings.ReplaceAll(urlStr, "https://", "")
	urlStr = strings.ReplaceAll(urlStr, "http://", "")
	urlStr = strings.ReplaceAll(urlStr, "/", "_")
	urlStr = strings.ReplaceAll(urlStr, "?", "_")
	urlStr = strings.ReplaceAll(urlStr, "&", "_")
	return urlStr
}
