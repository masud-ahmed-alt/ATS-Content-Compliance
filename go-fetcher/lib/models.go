package lib

// FetchRequest represents an incoming fetch request
type FetchRequest struct {
	Urls []string `json:"urls"`
}

// PageContent represents the content of a fetched page
type PageContent struct {
	URL         string `json:"url"`
	HTML        string `json:"html"`
	Error       string `json:"error,omitempty"`
	ContentType string `json:"content_type,omitempty"`
}

// PageBatch represents a batch of pages to send to analyzer
type PageBatch struct {
	RequestID  string        `json:"request_id"`
	MainURL    string        `json:"main_url"`
	BatchNum   int           `json:"batch_num"`
	Pages      []PageContent `json:"pages"`
	IsComplete bool          `json:"is_complete"`
}

// ProgressEvent represents a progress event for SSE
type ProgressEvent struct {
	Type      string  `json:"type"`
	RequestID string  `json:"request_id"`
	URL       string  `json:"url"`
	Done      int     `json:"done"`
	Total     int     `json:"total"`
	Percent   float64 `json:"percent"`
	Message   string  `json:"message,omitempty"`
}

// CrawlState tracks the state of crawling a single seed
type CrawlState struct {
	RequestID   string
	MainURL     string
	MainHost    string
	Visited     map[string]struct{}
	CurrentBatch []PageContent
	BatchNum    int
	Processed   int64
	Enqueued    int64
	MaxPages    int
}
