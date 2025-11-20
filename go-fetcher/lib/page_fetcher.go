package lib

import (
	"bytes"
	"io"
	"mime"
	"net/http"
	"strings"
)

// PageFetcher handles fetching pages from the web
type PageFetcher struct {
	httpClient  *http.Client
	maxPageBytes int64
}

// NewPageFetcher creates a new page fetcher
func NewPageFetcher(httpClient *http.Client, maxPageBytes int64) *PageFetcher {
	return &PageFetcher{
		httpClient:    httpClient,
		maxPageBytes:   maxPageBytes,
	}
}

// FetchPage fetches a page and returns its content
func (pf *PageFetcher) FetchPage(target string) PageContent {
	req, _ := http.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("User-Agent", "go-crawler/3.0 (+SSE)")
	resp, err := pf.httpClient.Do(req)
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
	_, _ = io.Copy(&buf, io.LimitReader(resp.Body, pf.maxPageBytes))
	html := buf.String()

	// HTML pages are no longer saved to MinIO in go-fetcher
	// They will be saved by python-analyzer only when hits are detected

	return PageContent{URL: target, HTML: html, ContentType: ct}
}
