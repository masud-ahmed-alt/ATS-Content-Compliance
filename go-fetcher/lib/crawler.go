package lib

import (
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"go-fetcher/utils"
)

// Crawler handles the crawling logic
type Crawler struct {
	pageFetcher    *PageFetcher
	analyzerClient *AnalyzerClient
	eventHub       *EventHub
	config         CrawlerConfig
}

// CrawlerConfig holds crawler configuration
type CrawlerConfig struct {
	BatchSize       int
	ProgressEveryN  int
	PerSeedWorkers  int
	MaxPagesPerSeed int
}

// NewCrawler creates a new crawler
func NewCrawler(pageFetcher *PageFetcher, analyzerClient *AnalyzerClient, eventHub *EventHub, config CrawlerConfig) *Crawler {
	return &Crawler{
		pageFetcher:    pageFetcher,
		analyzerClient: analyzerClient,
		eventHub:       eventHub,
		config:         config,
	}
}

// StartCrawl begins crawling the given URLs
func (c *Crawler) StartCrawl(requestID string, urls []string) {
	var wg sync.WaitGroup
	for _, u := range urls {
		if strings.TrimSpace(u) == "" {
			continue
		}
		wg.Add(1)
		go func(seed string) {
			defer wg.Done()
			c.eventHub.Publish(ProgressEvent{Type: "start", RequestID: requestID, URL: seed, Message: "started"})
			if err := c.CrawlOneSeed(requestID, seed); err != nil {
				c.eventHub.Publish(ProgressEvent{Type: "error", RequestID: requestID, URL: seed, Message: err.Error()})
			}
		}(u)
	}
	wg.Wait()
	c.eventHub.Publish(ProgressEvent{Type: "complete", RequestID: requestID, Message: "all seeds completed"})
}

// CrawlOneSeed crawls a single seed URL
func (c *Crawler) CrawlOneSeed(requestID, seed string) error {
	u, err := url.Parse(seed)
	if err != nil || u.Host == "" {
		return fmt.Errorf("invalid seed: %s", seed)
	}
	st := &crawlState{
		requestID:     requestID,
		mainURL:       seed,
		mainHost:      strings.ToLower(u.Host),
		visited:       make(map[string]struct{}),
		maxPages:      c.config.MaxPagesPerSeed,
		mu:            &sync.Mutex{},
		pages:         make([]PageContent, 0, c.config.MaxPagesPerSeed),
		pagesMu:       &sync.Mutex{},
	}

	urlQueue := make(chan string, 1024)
	var wg sync.WaitGroup

	enqueue := func(link string) {
		link = NormalizeURL(st.mainURL, link)
		if link == "" {
			return
		}
		lu, err := url.Parse(link)
		if err != nil || !utils.SameHost(st.mainHost, lu.Host) {
			return
		}

		st.mu.Lock()
		defer st.mu.Unlock()

		if len(st.visited) >= st.maxPages {
			return
		}

		if _, ok := st.visited[link]; ok {
			return
		}
		st.visited[link] = struct{}{}
		wg.Add(1)
		atomic.AddInt64(&st.enqueued, 1)
		urlQueue <- link
	}

	enqueue(seed)
	for i := 0; i < c.config.PerSeedWorkers; i++ {
		go func() {
			for u := range urlQueue {
				pc := c.pageFetcher.FetchPage(u)
				st.addPage(pc)
				done := int(atomic.AddInt64(&st.processed, 1))
				total := int(atomic.LoadInt64(&st.enqueued))
				if done%c.config.ProgressEveryN == 0 {
					c.eventHub.Publish(ProgressEvent{
						Type:      "progress",
						RequestID: requestID,
						URL:       seed,
						Done:      done, Total: total,
						Percent:   utils.Percent(done, total),
					})
				}

				if done >= st.maxPages {
					c.eventHub.Publish(ProgressEvent{
						Type:      "limit_reached",
						RequestID: requestID,
						URL:       seed,
						Message:   fmt.Sprintf("Reached max crawl limit of %d pages", st.maxPages),
					})
					wg.Done()
					return
				}

				if pc.Error == "" && strings.HasPrefix(pc.ContentType, "text/html") {
					for _, l := range ExtractSameDomainLinks(pc.HTML, u) {
						enqueue(l)
					}
				}
				wg.Done()
			}
		}()
	}

	wg.Wait()
	close(urlQueue)
	st.sendSingleBatch(c.analyzerClient)
	done := int(atomic.LoadInt64(&st.processed))
	total := int(atomic.LoadInt64(&st.enqueued))
	c.eventHub.Publish(ProgressEvent{
		Type:      "complete",
		RequestID: requestID,
		URL:       seed,
		Done:      done,
		Total:     total,
		Percent:   utils.Percent(done, total),
	})
	return nil
}

// crawlState holds state during crawling
type crawlState struct {
	requestID    string
	mainURL      string
	mainHost     string
	mu           *sync.Mutex
	visited      map[string]struct{}
	pagesMu      *sync.Mutex
	pages        []PageContent
	processed    int64
	enqueued     int64
	maxPages     int
}

func (st *crawlState) addPage(pc PageContent) {
	st.pagesMu.Lock()
	st.pages = append(st.pages, pc)
	st.pagesMu.Unlock()
}

func (st *crawlState) sendSingleBatch(ac *AnalyzerClient) {
	st.pagesMu.Lock()
	pagesCopy := make([]PageContent, len(st.pages))
	copy(pagesCopy, st.pages)
	st.pagesMu.Unlock()

	archive, metadata, stats, err := buildCompressedArchive(st.mainURL, pagesCopy)
	if err != nil {
		log.Printf("[crawler:error] failed to compress archive for %s: %v", st.mainURL, err)
		return
	}

	batch := PageBatch{
		RequestID:     st.requestID,
		BatchID:       uuid.New().String(),
		MainURL:       st.mainURL,
		BatchNum:      1,
		IsComplete:    true,
		TotalPages:    len(metadata),
		ArchiveBase64: archive,
		Compression:   "zip-base64",
		Metadata:      metadata,
		Stats:         stats,
	}

	go func(b PageBatch) {
		if err := ac.SendBatch(b); err != nil {
			log.Printf("[crawler:error] batch delivery failed for %s: %v", b.BatchID, err)
		}
	}(batch)
}
