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
		MaxGlobalCrawls: utils.EnvInt("WORKERS", 128),
		PerSeedWorkers:  utils.EnvInt("PER_SEED_WORKERS", 16),
		MaxPagesPerSeed: utils.EnvInt("MAX_PAGES_PER_SEED", 1000),
		AnalyzerURL:     utils.GetEnv("ANALYZER_URL", "http://python-analyzer:8000/ingest"),
		AnalyzerConc:    utils.EnvInt("ANALYZER_CONCURRENCY", 8),
		AnalyzerGzip:    utils.EnvInt("ANALYZER_GZIP", 1) == 1,
		MinioEndpoint:   utils.GetEnv("MINIO_ENDPOINT", "minio:7000"),
		MinioAccessKey:  utils.GetEnv("MINIO_ACCESS_KEY", "admin"),
		MinioSecretKey:  utils.GetEnv("MINIO_SECRET_KEY", "minioadmin"),
		MinioUseSSL:     utils.GetEnv("MINIO_USE_SSL", "false") == "true",
		MinioBucket:     utils.GetEnv("MINIO_BUCKET", "crawler-pages"),
		AllowedOrigin:   utils.GetEnv("ALLOWED_ORIGIN", "*"),
	}

	// Setup HTTP clients
	cfg.HTTPClient = &http.Client{
		Timeout: cfg.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	cfg.AnalyzerClient = &http.Client{
		Timeout: 120 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	AppConfig = cfg
	return cfg
}
