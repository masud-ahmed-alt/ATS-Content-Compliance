// go-crawler-sse.go
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
)

type FetchRequest struct {
	Urls []string `json:"urls"`
}

type PageContent struct {
	URL         string `json:"url"`
	HTML        string `json:"html"`
	Error       string `json:"error,omitempty"`
	ContentType string `json:"content_type,omitempty"`
}

type PageBatch struct {
	RequestID  string        `json:"request_id"`
	MainURL    string        `json:"main_url"`
	BatchNum   int           `json:"batch_num"`
	Pages      []PageContent `json:"pages"`
	IsComplete bool          `json:"is_complete"`
}

type ProgressEvent struct {
	Type      string  `json:"type"`
	RequestID string  `json:"request_id"`
	URL       string  `json:"url"`
	Done      int     `json:"done"`
	Total     int     `json:"total"`
	Percent   float64 `json:"percent"`
	Message   string  `json:"message,omitempty"`
}

// ===== Environment Config =====

var (
	timeout             = time.Duration(envInt("TIMEOUT_SECS", 20)) * time.Second
	maxPageBytes  int64 = int64(envInt("MAX_PAGE_BYTES", 2*1024*1024)) // 2MB limit
	batchSize           = envInt("BATCH_SIZE", 50)
	progressEveryN      = envInt("PROGRESS_EVERY_N", 10)
	maxGlobalCrawls     = envInt("WORKERS", 128)
	perSeedWorkers      = envInt("PER_SEED_WORKERS", 16)
	analyzerURL         = getEnv("ANALYZER_URL", "http://python-analyzer:8000/ingest")
	analyzerConc        = envInt("ANALYZER_CONCURRENCY", 8)
	analyzerGzip        = envInt("ANALYZER_GZIP", 1) == 1

	httpClient = &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	analyzerClient = &http.Client{
		Timeout: 120 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	globalCrawlSem = make(chan struct{}, maxGlobalCrawls)
	analyzerSem    = make(chan struct{}, analyzerConc)

	hub = newEventHub()
)

// ===== SSE Hub =====

type subscriber struct {
	ch   chan ProgressEvent
	done chan struct{}
}

type eventHub struct {
	mu        sync.RWMutex
	requestCh map[string]map[*subscriber]struct{} // "" = global subscribers
}

func newEventHub() *eventHub {
	return &eventHub{requestCh: make(map[string]map[*subscriber]struct{})}
}

func (h *eventHub) subscribe(requestID string) *subscriber {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := &subscriber{ch: make(chan ProgressEvent, 256), done: make(chan struct{})}
	if _, ok := h.requestCh[requestID]; !ok {
		h.requestCh[requestID] = make(map[*subscriber]struct{})
	}
	h.requestCh[requestID][s] = struct{}{}
	return s
}

func (h *eventHub) unsubscribe(requestID string, s *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs, ok := h.requestCh[requestID]; ok {
		delete(subs, s)
		close(s.ch)
		close(s.done)
		if len(subs) == 0 {
			delete(h.requestCh, requestID)
		}
	}
}

func (h *eventHub) publish(ev ProgressEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, subs := range []map[*subscriber]struct{}{
		h.requestCh[""], // global
		h.requestCh[ev.RequestID],
	} {
		for s := range subs {
			select {
			case s.ch <- ev:
			default: // drop if slow
			}
		}
	}
}

// ===== HTTP Handlers =====

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/fetch", withCORS(handleFetch))
	mux.HandleFunc("/events", withCORS(handleSSEAll))         // all requests
	mux.HandleFunc("/events/", withCORS(handleSSEByRequest))   // specific request

	port := getEnv("PORT", "8080")
	log.Printf("go-crawler (SSE) running on :%s [workers=%d, per_seed=%d, batch=%d, analyzer_conc=%d]",
		port, maxGlobalCrawls, perSeedWorkers, batchSize, analyzerConc)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", getEnv("ALLOWED_ORIGIN", "*"))
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

// POST /fetch
func handleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req FetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Urls) == 0 {
		http.Error(w, "invalid JSON or empty urls", http.StatusBadRequest)
		return
	}

	requestID := uuid.NewString()
	go startCrawl(requestID, req.Urls)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":     "started",
		"request_id": requestID,
	})
}

// GET /events → global feed
func handleSSEAll(w http.ResponseWriter, r *http.Request) {
	streamSSE(w, r, "")
}

// GET /events/{request_id}
func handleSSEByRequest(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/events/")
	if id == "" {
		http.Error(w, "missing request_id", http.StatusBadRequest)
		return
	}
	streamSSE(w, r, id)
}

func streamSSE(w http.ResponseWriter, r *http.Request, requestID string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream unsupported", http.StatusInternalServerError)
		return
	}

	sub := hub.subscribe(requestID)
	defer hub.unsubscribe(requestID, sub)

	bw := bufio.NewWriter(w)
	fmt.Fprintf(bw, "event: connected\ndata: {}\n\n")
	bw.Flush()
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-sub.ch:
			data, _ := json.Marshal(ev)
			fmt.Fprintf(bw, "event: %s\ndata: %s\n\n", ev.Type, data)
			bw.Flush()
			flusher.Flush()
		}
	}
}

// ===== Crawl Execution =====

func startCrawl(reqID string, urls []string) {
	var wg sync.WaitGroup
	for _, u := range urls {
		if strings.TrimSpace(u) == "" {
			continue
		}
		wg.Add(1)
		go func(seed string) {
			defer wg.Done()
			globalCrawlSem <- struct{}{}
			defer func() { <-globalCrawlSem }()
			hub.publish(ProgressEvent{Type: "start", RequestID: reqID, URL: seed, Message: "started"})
			if err := crawlOneSeed(reqID, seed); err != nil {
				hub.publish(ProgressEvent{Type: "error", RequestID: reqID, URL: seed, Message: err.Error()})
			}
		}(u)
	}
	wg.Wait()
	hub.publish(ProgressEvent{Type: "complete", RequestID: reqID, Message: "all seeds completed"})
}

// ===== Crawl per seed =====

type crawlState struct {
	requestID string
	mainURL   string
	mainHost  string

	muVisited sync.Mutex
	visited   map[string]struct{}

	muBatch      sync.Mutex
	currentBatch []PageContent
	batchNum     int

	processed int64
	enqueued  int64
}

func crawlOneSeed(requestID, seed string) error {
	u, err := url.Parse(seed)
	if err != nil || u.Host == "" {
		return fmt.Errorf("invalid seed: %s", seed)
	}
	st := &crawlState{
		requestID:    requestID,
		mainURL:      seed,
		mainHost:     strings.ToLower(u.Host),
		visited:      make(map[string]struct{}),
		currentBatch: make([]PageContent, 0, batchSize),
	}
	urlQueue := make(chan string, 1024)
	var wg sync.WaitGroup

	enqueue := func(link string) {
		link = normalizeURL(st.mainURL, link)
		if link == "" {
			return
		}
		lu, err := url.Parse(link)
		if err != nil || !sameHost(st.mainHost, lu.Host) {
			return
		}
		st.muVisited.Lock()
		if _, ok := st.visited[link]; ok {
			st.muVisited.Unlock()
			return
		}
		st.visited[link] = struct{}{}
		st.muVisited.Unlock()
		wg.Add(1)
		atomic.AddInt64(&st.enqueued, 1)
		urlQueue <- link
	}

	enqueue(seed)
	for i := 0; i < perSeedWorkers; i++ {
		go func() {
			for u := range urlQueue {
				pc := fetchPage(u)
				st.addToBatch(pc)
				done := int(atomic.AddInt64(&st.processed, 1))
				total := int(atomic.LoadInt64(&st.enqueued))
				if done%progressEveryN == 0 {
					hub.publish(ProgressEvent{
						Type:      "progress",
						RequestID: requestID,
						URL:       seed,
						Done:      done, Total: total,
						Percent: percent(done, total),
					})
				}
				if pc.Error == "" && strings.HasPrefix(pc.ContentType, "text/html") {
					for _, l := range extractSameDomainLinks(pc.HTML, u) {
						enqueue(l)
					}
				}
				wg.Done()
			}
		}()
	}

	wg.Wait()
	close(urlQueue)
	st.flushBatch(true)
	done := int(atomic.LoadInt64(&st.processed))
	total := int(atomic.LoadInt64(&st.enqueued))
	hub.publish(ProgressEvent{
		Type: "complete", RequestID: requestID, URL: seed,
		Done: done, Total: total, Percent: percent(done, total),
	})
	return nil
}

// ===== Analyzer Integration =====

func (st *crawlState) addToBatch(pc PageContent) {
	st.muBatch.Lock()
	defer st.muBatch.Unlock()
	st.currentBatch = append(st.currentBatch, pc)
	if len(st.currentBatch) >= batchSize {
		st.flushBatchUnsafe(false)
	}
}

func (st *crawlState) flushBatchUnsafe(isComplete bool) {
	if len(st.currentBatch) == 0 {
		return
	}
	st.batchNum++
	batch := PageBatch{
		RequestID:  st.requestID,
		MainURL:    st.mainURL,
		BatchNum:   st.batchNum,
		Pages:      st.currentBatch,
		IsComplete: isComplete,
	}
	go sendBatchToAnalyzer(batch)
	st.currentBatch = make([]PageContent, 0, batchSize)
}

func (st *crawlState) flushBatch(isComplete bool) {
	st.muBatch.Lock()
	defer st.muBatch.Unlock()
	st.flushBatchUnsafe(isComplete)
}

func sendBatchToAnalyzer(batch PageBatch) {
	analyzerSem <- struct{}{}
	defer func() { <-analyzerSem }()

	var buf bytes.Buffer
	var body io.Reader

	if analyzerGzip {
		gzw := gzip.NewWriter(&buf)
		json.NewEncoder(gzw).Encode(batch)
		gzw.Close()
		body = bytes.NewReader(buf.Bytes())
	} else {
		json.NewEncoder(&buf).Encode(batch)
		body = bytes.NewReader(buf.Bytes())
	}

	req, _ := http.NewRequest(http.MethodPost, analyzerURL, body)
	req.Header.Set("Content-Type", "application/json")
	if analyzerGzip {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := analyzerClient.Do(req)
	if err == nil && resp != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// ===== Utilities =====

func fetchPage(target string) PageContent {
	req, _ := http.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("User-Agent", "go-crawler/3.0 (+SSE)")
	resp, err := httpClient.Do(req)
	if err != nil {
		return PageContent{URL: target, Error: err.Error()}
	}
	defer resp.Body.Close()
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if ct != "" {
		if mediatype, _, err := mime.ParseMediaType(ct); err == nil {
			ct = mediatype
		}
	}
	var buf bytes.Buffer
	io.Copy(&buf, io.LimitReader(resp.Body, maxPageBytes))
	return PageContent{URL: target, HTML: buf.String(), ContentType: ct}
}

func extractSameDomainLinks(htmlStr, baseURL string) []string {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlStr))
	if err != nil {
		return nil
	}
	base, _ := url.Parse(baseURL)
	var links []string
	doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		parsed, err := base.Parse(href)
		if err == nil && strings.EqualFold(parsed.Host, base.Host) {
			links = append(links, parsed.String())
		}
	})
	return links
}

func percent(done, total int) float64 {
	if total <= 0 {
		return 0
	}
	return (float64(done) / float64(total)) * 100.0
}

func normalizeURL(baseURL, href string) string {
	if href == "" || strings.HasPrefix(href, "javascript:") {
		return ""
	}
	bu, err := url.Parse(baseURL)
	if err != nil {
		return ""
	}
	pu, err := bu.Parse(href)
	if err != nil {
		return ""
	}
	pu.Fragment = ""
	return pu.String()
}

func sameHost(a, b string) bool { return strings.EqualFold(a, b) }

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		fmt.Sscanf(v, "%d", &n)
		if n > 0 {
			return n
		}
	}
	return fallback
}
