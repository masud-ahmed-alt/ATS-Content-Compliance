package lib

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"log"
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
		log.Printf("MinIO upload failed for %s: %v", urlStr, err)
	} else {
		log.Printf("Saved page to MinIO: %s", objectName)
	}
}

// SanitizeFilenameForMinIO converts URL to a safe filename
func SanitizeFilenameForMinIO(urlStr string) string {
	urlStr = strings.ReplaceAll(urlStr, "https://", "")
	urlStr = strings.ReplaceAll(urlStr, "http://", "")
	urlStr = strings.ReplaceAll(urlStr, "/", "_")
	urlStr = strings.ReplaceAll(urlStr, "?", "_")
	urlStr = strings.ReplaceAll(urlStr, "&", "_")
	return urlStr
}
