package main

import (
	"context"
	"log"
	"net/http"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"go-fetcher/config"
	"go-fetcher/lib"
)

var (
	cfg           *config.Config
	eventHub      *lib.EventHub
	pageFetcher   *lib.PageFetcher
	analyzerCli   *lib.AnalyzerClient
	crawler       *lib.Crawler
	handler       *lib.Handler
	minioClient   *minio.Client
	minioUploader *lib.MinIOUploader
)

func init() {
	// Initialize configuration from environment variables
	cfg = config.Init()

	// Initialize MinIO client for cloud storage
	var err error
	minioClient, err = minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: cfg.MinioUseSSL,
	})
	if err != nil {
		log.Fatalf("MinIO init failed: %v", err)
	}

	// Verify MinIO connection and create bucket if needed
	ctx := context.Background()
	exists, err := minioClient.BucketExists(ctx, cfg.MinioBucket)
	if err != nil {
		log.Fatalf("MinIO connection failed: %v", err)
	}
	if !exists {
		err = minioClient.MakeBucket(ctx, cfg.MinioBucket, minio.MakeBucketOptions{})
		if err != nil {
			log.Fatalf("Failed to create MinIO bucket: %v", err)
		}
	}
	log.Printf("Connected to MinIO endpoint=%s bucket=%s", cfg.MinioEndpoint, cfg.MinioBucket)

	// Initialize all application components
	eventHub = lib.NewEventHub()
	minioUploader = lib.NewMinIOUploader(minioClient, cfg.MinioBucket)
	pageFetcher = lib.NewPageFetcher(cfg.HTTPClient, cfg.MaxPageBytes, minioUploader)
	analyzerCli = lib.NewAnalyzerClient(cfg.AnalyzerClient, cfg.AnalyzerURL, cfg.AnalyzerGzip)
	crawler = lib.NewCrawler(pageFetcher, analyzerCli, eventHub, lib.CrawlerConfig{
		BatchSize:       cfg.BatchSize,
		ProgressEveryN:  cfg.ProgressEveryN,
		PerSeedWorkers:  cfg.PerSeedWorkers,
		MaxPagesPerSeed: cfg.MaxPagesPerSeed,
	})
	handler = lib.NewHandler(crawler, eventHub, cfg.MaxGlobalCrawls)
}

func main() {
	// Setup HTTP routes with CORS middleware
	mux := http.NewServeMux()
	mux.HandleFunc("/fetch", lib.WithCORS(cfg.AllowedOrigin, handler.HandleFetch))
	mux.HandleFunc("/events", lib.WithCORS(cfg.AllowedOrigin, handler.HandleSSEAll))
	mux.HandleFunc("/events/", lib.WithCORS(cfg.AllowedOrigin, handler.HandleSSEByRequest))

	// Start server
	log.Printf("go-crawler (SSE) running on :%s [workers=%d, per_seed=%d, batch=%d, max_pages=%d, analyzer_conc=%d]",
		cfg.Port, cfg.MaxGlobalCrawls, cfg.PerSeedWorkers, cfg.BatchSize, cfg.MaxPagesPerSeed, cfg.AnalyzerConc)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, mux))
}
