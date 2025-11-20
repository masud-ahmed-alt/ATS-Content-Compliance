package config

import (
	"net/http"
	"time"
	"go-fetcher/utils"
)

// Config holds all application configuration
type Config struct {
	// HTTP Settings
	Timeout time.Duration
	Port    string

	// Crawler Settings
	MaxPageBytes    int64
	BatchSize       int
	ProgressEveryN  int
	MaxGlobalCrawls int
	PerSeedWorkers  int
	MaxPagesPerSeed int

	// Analyzer Settings
	AnalyzerURL    string
	AnalyzerConc   int
	AnalyzerGzip   bool

	// MinIO Settings
	MinioEndpoint string
	MinioAccessKey string
	MinioSecretKey string
	MinioUseSSL    bool
	MinioBucket   string

	// Redis Settings (for Dead Letter Queue)
	RedisAddr string

	// HTTP Clients
	HTTPClient    *http.Client
	AnalyzerClient *http.Client

	// CORS
	AllowedOrigin string
}

// Global config instance
var AppConfig *Config

// Init initializes the global config
func Init() *Config {
	cfg := &Config{
		Timeout:         time.Duration(utils.EnvInt("TIMEOUT_SECS", 20)) * time.Second,
		Port:            utils.GetEnv("PORT", "8080"),
		MaxPageBytes:    int64(utils.EnvInt("MAX_PAGE_BYTES", 2*1024*1024)),
		BatchSize:       utils.EnvInt("BATCH_SIZE", 50),
		ProgressEveryN:  utils.EnvInt("PROGRESS_EVERY_N", 10),
		// Dynamic workers: Default to 128, but can be overridden via env var
		// For 12GB system, recommend 256; for 16GB+, can use 512
		MaxGlobalCrawls: utils.EnvInt("WORKERS", 128),
		// Dynamic per-seed workers: Scale based on system (default 8, can increase if more resources)
		PerSeedWorkers:  utils.EnvInt("PER_SEED_WORKERS", 8),
		MaxPagesPerSeed: utils.EnvInt("MAX_PAGES_PER_SEED", 1000),
		AnalyzerURL:     utils.GetEnv("PYTHON_ANALYZER_URL", "http://python-analyzer:8000/webhook/task_done"),
		AnalyzerConc:    utils.EnvInt("ANALYZER_CONCURRENCY", 8),
		AnalyzerGzip:    utils.EnvInt("ANALYZER_GZIP", 1) == 1,
		MinioEndpoint:   utils.GetEnv("MINIO_ENDPOINT", "minio:7000"),
		MinioAccessKey:  utils.GetEnv("MINIO_ACCESS_KEY", "admin"),
		MinioSecretKey:  utils.GetEnv("MINIO_SECRET_KEY", "minioadmin"),
		MinioUseSSL:     utils.GetEnv("MINIO_USE_SSL", "false") == "true",
		MinioBucket:     utils.GetEnv("MINIO_BUCKET", "crawler-pages"),
		RedisAddr:       utils.GetEnv("REDIS_ADDR", "redis:6379"),
		AllowedOrigin:   utils.GetEnv("ALLOWED_ORIGIN", "*"),
	}

	// Setup HTTP clients
	cfg.HTTPClient = &http.Client{
		Timeout: cfg.Timeout,
		// Dynamic connection pool: Can scale up if more resources available
		// Defaults are conservative for 12GB system, but can be increased via env vars
		Transport: &http.Transport{
			MaxIdleConns:        utils.EnvInt("HTTP_MAX_IDLE_CONNS", 256),
			MaxIdleConnsPerHost: utils.EnvInt("HTTP_MAX_IDLE_CONNS_PER_HOST", 64),
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Increased timeout for large batches - analyzer needs time to read and process large payloads
	analyzerTimeout := time.Duration(utils.EnvInt("ANALYZER_TIMEOUT_SECS", 180)) * time.Second
	cfg.AnalyzerClient = &http.Client{
		Timeout: analyzerTimeout,
		// Dynamic connection pool: Can scale up if more resources available
		Transport: &http.Transport{
			MaxIdleConns:        utils.EnvInt("ANALYZER_MAX_IDLE_CONNS", 256),
			MaxIdleConnsPerHost: utils.EnvInt("ANALYZER_MAX_IDLE_CONNS_PER_HOST", 64),
			IdleConnTimeout:     90 * time.Second,
			ResponseHeaderTimeout: 180 * time.Second, // Timeout for reading response headers
		},
	}

	AppConfig = cfg
	return cfg
}
