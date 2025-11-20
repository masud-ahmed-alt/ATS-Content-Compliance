package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"go-fetcher/config"
	"go-fetcher/lib"
)

var (
	cfg              *config.Config
	eventHub         *lib.EventHub
	pageFetcher      *lib.PageFetcher
	analyzerCli      *lib.AnalyzerClient
	crawler          *lib.Crawler
	handler          *lib.Handler
	redisClient      *redis.Client
	deadLetterQueue  *lib.DeadLetterQueue
)

func init() {
	// Initialize configuration from environment variables
	cfg = config.Init()

	// MinIO removed from go-fetcher - HTML pages are now saved by python-analyzer only when hits are detected

	// Initialize Redis client for dead letter queue
	redisClient = redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		MaxRetries:   3,
		PoolSize:     10,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})

	// Test Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("⚠️ Warning: Redis unavailable at %s: %v. DLQ disabled.", cfg.RedisAddr, err)
		redisClient = nil
	} else {
		log.Printf("✅ Redis connected at %s", cfg.RedisAddr)
	}

	// Initialize all application components
	eventHub = lib.NewEventHub()
	pageFetcher = lib.NewPageFetcher(cfg.HTTPClient, cfg.MaxPageBytes)
	analyzerCli = lib.NewAnalyzerClient(cfg.AnalyzerClient, cfg.AnalyzerURL, cfg.AnalyzerGzip)
	analyzerCli.SetEventHub(eventHub)  // ✅ Set event hub for failure notifications
	crawler = lib.NewCrawler(pageFetcher, analyzerCli, eventHub, lib.CrawlerConfig{
		BatchSize:       cfg.BatchSize,
		ProgressEveryN:  cfg.ProgressEveryN,
		PerSeedWorkers:  cfg.PerSeedWorkers,
		MaxPagesPerSeed: cfg.MaxPagesPerSeed,
	})
	handler = lib.NewHandler(crawler, eventHub, cfg.MaxGlobalCrawls)

	// Initialize Dead Letter Queue for failed batch delivery
	if redisClient != nil {
		deadLetterQueue = lib.NewDeadLetterQueue(redisClient, "dlq:failed-batches")
		log.Printf("✅ Dead Letter Queue initialized")
	} else {
		log.Printf("⚠️ Dead Letter Queue disabled (Redis unavailable)")
	}
}

// startDLQWorker periodically retries failed batches from the DLQ
func startDLQWorker() {
	if deadLetterQueue == nil {
		return
	}

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			stats := deadLetterQueue.Stats()
			size := stats["size"].(int64)

			if size > 0 {
				log.Printf("[dlq:worker] Processing %d failed batches", size)
				if err := deadLetterQueue.RetryFailedBatches(analyzerCli); err != nil {
					log.Printf("[dlq:worker:error] %v", err)
				}
			}
		}
	}()
}

func main() {
	// Start DLQ worker for periodic retry
	startDLQWorker()

	// Setup HTTP routes with CORS middleware
	mux := http.NewServeMux()
	mux.HandleFunc("/fetch", lib.WithCORS(cfg.AllowedOrigin, handler.HandleFetch))
	mux.HandleFunc("/events", lib.WithCORS(cfg.AllowedOrigin, handler.HandleSSEAll))
	mux.HandleFunc("/events/", lib.WithCORS(cfg.AllowedOrigin, handler.HandleSSEByRequest))
	mux.HandleFunc("/active", lib.WithCORS(cfg.AllowedOrigin, handler.HandleActiveRequests))

	// Start server
	log.Printf("go-crawler (SSE) running on :%s [workers=%d, per_seed=%d, batch=%d, max_pages=%d, analyzer_conc=%d]",
		cfg.Port, cfg.MaxGlobalCrawls, cfg.PerSeedWorkers, cfg.BatchSize, cfg.MaxPagesPerSeed, cfg.AnalyzerConc)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, mux))
}
