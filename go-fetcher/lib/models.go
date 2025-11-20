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

// PageMetadata describes an entry embedded inside the compressed archive
type PageMetadata struct {
	URL         string `json:"url"`
	FileName    string `json:"file_name,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	Error       string `json:"error,omitempty"`
	SizeBytes   int    `json:"size_bytes,omitempty"`
	HasHTML     bool   `json:"has_html"`
}

// BatchStats provides a quick overview of successes and failures
type BatchStats struct {
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

// PageBatch represents a batch of pages to send to analyzer
type PageBatch struct {
	RequestID      string        `json:"request_id"`
	BatchID        string        `json:"batch_id"`
	MainURL        string        `json:"main_url"`
	BatchNum       int           `json:"batch_num"`
	Pages          []PageContent `json:"pages,omitempty"`
	IsComplete     bool          `json:"is_complete"`
	TotalPages     int           `json:"total_pages"`
	ArchiveBase64  string        `json:"archive_base64,omitempty"`
	Compression    string        `json:"compression,omitempty"`
	Metadata       []PageMetadata `json:"metadata,omitempty"`
	Stats          BatchStats    `json:"stats,omitempty"`
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
