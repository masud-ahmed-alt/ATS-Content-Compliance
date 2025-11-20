package lib

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// MinIOUploader handles MinIO operations
type MinIOUploader struct {
	client *minio.Client
	bucket string
}

// NewMinIOUploader creates a new MinIO uploader
func NewMinIOUploader(client *minio.Client, bucket string) *MinIOUploader {
	return &MinIOUploader{
		client: client,
		bucket: bucket,
	}
}

// UploadPage uploads a page to MinIO with gzip compression
func (m *MinIOUploader) UploadPage(urlStr string, data []byte) {
	ctx := context.Background()
	objectName := fmt.Sprintf("%s_%d.html.gz", SanitizeFilenameForMinIO(urlStr), time.Now().UnixNano())

	var gzBuf bytes.Buffer
	gzw := gzip.NewWriter(&gzBuf)
	if _, err := gzw.Write(data); err != nil {
		log.Printf("gzip error for %s: %v", urlStr, err)
		_ = gzw.Close()
		return
	}
	_ = gzw.Close()

	_, err := m.client.PutObject(
		ctx,
		m.bucket,
		objectName,
		bytes.NewReader(gzBuf.Bytes()),
		int64(gzBuf.Len()),
		minio.PutObjectOptions{
			ContentType:     "text/html",
			ContentEncoding: "gzip",
		},
	)
	if err != nil {
		log.Printf("MinIO upload failed for %s: objectName=%s error=%v", urlStr, objectName, err)
	} else {
		log.Printf("Saved page to MinIO: %s (from %s)", objectName, urlStr)
	}
}

// SanitizeFilenameForMinIO converts URL to a safe filename using hashing for long URLs
func SanitizeFilenameForMinIO(urlStr string) string {
	// Parse URL to extract domain and path safely
	u, err := url.Parse(urlStr)
	if err != nil {
		// Fallback to hash if URL parsing fails
		hash := sha256.Sum256([]byte(urlStr))
		return fmt.Sprintf("page_%x", hash[:8])
	}

	// Create base filename from domain
	domain := strings.ReplaceAll(u.Hostname(), ".", "_")
	path := u.Path
	
	// Remove leading slash and replace problematic characters
	path = strings.TrimPrefix(path, "/")
	path = strings.ReplaceAll(path, "/", "_")
	path = strings.ReplaceAll(path, "-", "_")
	
	// Limit to first 50 chars of path to keep it readable
	if len(path) > 50 {
		path = path[:50]
	}

	// Create base filename: domain_path
	baseName := domain
	if path != "" {
		baseName = baseName + "_" + path
	}

	// If total length is still too long, use hash of full URL + domain
	if len(baseName) > 200 {
		// Hash the query string and fragment separately
		hash := sha256.Sum256([]byte(urlStr))
		baseName = fmt.Sprintf("%s_%x", domain, hash[:8])
	}

	// Final cleanup: remove any remaining special characters MinIO might not like
	baseName = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, baseName)

	// Ensure reasonable length (MinIO limit is typically 1024, but let's be safe)
	if len(baseName) > 250 {
		baseName = baseName[:250]
	}

	return baseName
}
