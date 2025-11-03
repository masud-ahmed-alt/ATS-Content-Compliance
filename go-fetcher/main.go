package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// ============================
// Data Models
// ============================

// FetchRequest — POST body from client
type FetchRequest struct {
	Urls []string `json:"urls"`
}

// PageContent — single crawled page
type PageContent struct {
	URL   string `json:"url"`
	HTML  string `json:"html"`
	Error string `json:"error,omitempty"`
}

// CrawlTask — job info persisted in Redis
type CrawlTask struct {
	ID         string        `json:"id"`
	MainURL    string        `json:"main_url"`
	Status     string        `json:"status"`
	Pages      []PageContent `json:"pages"`
	StartedAt  time.Time     `json:"started_at"`
	EndedAt    time.Time     `json:"ended_at,omitempty"`
	TotalPages int           `json:"total_pages"`
}

// ============================
// Global Vars
// ============================

var (
	ctx       = context.Background()
	redisConn *redis.Client
	timeout   = 10 * time.Second

	analyzerEndpoint = "http://python-analyzer:8000/ingest"
)

const (
	taskPrefix  = "crawl_task:"
	taskListKey = "crawl_tasks"
	queuePrefix = "crawl_queue:"
)

// ============================
// Main Entry
// ============================

func main() {
	redisConn = redis.NewClient(&redis.Options{
		Addr:     getEnv("REDIS_HOST", "localhost:6379"),
		Password: "",
		DB:       0,
	})
	if err := redisConn.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}

	http.HandleFunc("/fetch", withCORS(handleFetch))
	http.HandleFunc("/task/", withCORS(handleTask))
	http.HandleFunc("/tasks", withCORS(handleTaskList))

	port := getEnv("PORT", "8080")
	log.Printf("go-crawler running on port %s (UNLIMITED CRAWL)", port)

	go queueWorker() // background worker
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// ============================
// Middleware
// ============================

func withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		allowed := getEnv("ALLOWED_ORIGIN", "*")
		w.Header().Set("Access-Control-Allow-Origin", allowed)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

// ============================
// Handlers
// ============================

// POST /fetch — accepts URLs and queues crawl jobs
func handleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req FetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if len(req.Urls) == 0 {
		sendError(w, "No URLs provided", http.StatusBadRequest)
		return
	}

	taskIDs := make([]string, 0, len(req.Urls))
	for _, u := range req.Urls {
		taskID := uuid.New().String()
		task := CrawlTask{
			ID:        taskID,
			MainURL:   u,
			Status:    "queued",
			StartedAt: time.Now(),
		}
		data, _ := json.Marshal(task)
		redisConn.Set(ctx, taskPrefix+taskID, data, 0)
		redisConn.LPush(ctx, taskListKey, taskID)

		// queue URL for worker
		queueKey := queuePrefix + taskID
		urlBytes, _ := json.Marshal([]string{u})
		redisConn.LPush(ctx, queueKey, urlBytes)
		taskIDs = append(taskIDs, taskID)
	}

	sendJSON(w, map[string]any{
		"status":     "batch queued",
		"task_ids":   taskIDs,
		"total_urls": len(req.Urls),
	}, http.StatusAccepted)
}

// GET /task/{id} — returns crawl result without HTML
func handleTask(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/task/")
	if id == "" {
		sendError(w, "Missing task ID", http.StatusBadRequest)
		return
	}
	data, err := redisConn.Get(ctx, taskPrefix+id).Result()
	if err == redis.Nil {
		sendError(w, "Task not found", http.StatusNotFound)
		return
	} else if err != nil {
		sendError(w, "Redis error", http.StatusInternalServerError)
		return
	}

	var task CrawlTask
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		sendError(w, "Failed to parse task", http.StatusInternalServerError)
		return
	}
	for i := range task.Pages {
		task.Pages[i].HTML = "" // remove heavy data
	}
	sendJSON(w, task, http.StatusOK)
}

// GET /tasks — list latest crawl jobs
func handleTaskList(w http.ResponseWriter, r *http.Request) {
	ids, _ := redisConn.LRange(ctx, taskListKey, 0, 50).Result()
	tasks := []CrawlTask{}
	for _, id := range ids {
		data, err := redisConn.Get(ctx, taskPrefix+id).Result()
		if err != nil {
			continue
		}
		var t CrawlTask
		if err := json.Unmarshal([]byte(data), &t); err == nil {
			t.Pages = nil
			tasks = append(tasks, t)
		}
	}
	sendJSON(w, tasks, http.StatusOK)
}

// ============================
// Core Crawl Worker
// ============================

func queueWorker() {
	log.Println("Queue worker started")
	for {
		keys, _ := redisConn.Keys(ctx, queuePrefix+"*").Result()
		for _, key := range keys {
			taskID := strings.TrimPrefix(key, queuePrefix)
			go processCrawlTask(taskID)
		}
		time.Sleep(time.Second)
	}
}

func processCrawlTask(taskID string) {
	data, err := redisConn.Get(ctx, taskPrefix+taskID).Result()
	if err != nil {
		return
	}

	var task CrawlTask
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		return
	}
	if task.Status != "queued" {
		return
	}

	task.Status = "processing"
	saveTask(taskID, &task)

	visited := make(map[string]bool)
	queue := []string{task.MainURL}
	results := []PageContent{}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if visited[current] {
			continue
		}
		visited[current] = true

		page := fetchPage(current)
		results = append(results, page)

		if page.Error == "" {
			links := extractSameDomainLinks(page.HTML, current)
			for _, link := range links {
				if !visited[link] {
					queue = append(queue, link)
				}
			}
		}

		// Update Redis progress after each page
		task.Pages = results
		task.TotalPages = len(results)
		saveTask(taskID, &task)
	}

	task.Status = "completed"
	task.EndedAt = time.Now()
	saveTask(taskID, &task)

	sendToAnalyzer(task)
	log.Printf("[task:%s] Completed %d pages", task.ID, len(task.Pages))
}

// ============================
// Fetch + Link Extraction
// ============================

func fetchPage(target string) PageContent {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(target)
	if err != nil {
		return PageContent{URL: target, Error: err.Error()}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	return PageContent{URL: target, HTML: string(body)}
}

func extractSameDomainLinks(html, baseURL string) []string {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil
	}

	links := []string{}
	doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		href = strings.TrimSpace(href)
		if href == "" {
			return
		}
		parsed, err := base.Parse(href)
		if err == nil && parsed.Host == base.Host {
			links = append(links, parsed.String())
		}
	})
	return links
}

// ============================
// Analyzer Integration
// ============================

func sendToAnalyzer(task CrawlTask) {
	payload, _ := json.Marshal(task)
	resp, err := http.Post(analyzerEndpoint, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		log.Printf("Analyzer POST failed for %s: %v", task.ID, err)
		return
	}
	defer resp.Body.Close()
	log.Printf("Analyzer notified: %s (HTTP %d)", task.ID, resp.StatusCode)
}

// ============================
// Utilities
// ============================

func saveTask(taskID string, task *CrawlTask) {
	data, _ := json.Marshal(task)
	redisConn.Set(ctx, taskPrefix+taskID, data, 0)
}

func sendError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func sendJSON(w http.ResponseWriter, data any, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(data)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
