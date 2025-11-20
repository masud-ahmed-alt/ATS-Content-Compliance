package lib

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
)

var fileNameSanitizer = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// buildCompressedArchive converts the crawled HTML pages into a zipped, base64-encoded payload.
func buildCompressedArchive(mainURL string, pages []PageContent) (string, []PageMetadata, BatchStats, error) {
	buf := bytes.NewBuffer(nil)
	zw := zip.NewWriter(buf)

	metadata := make([]PageMetadata, 0, len(pages))
	stats := BatchStats{}

	for idx, page := range pages {
		meta := PageMetadata{
			URL:         page.URL,
			ContentType: page.ContentType,
			Error:       page.Error,
		}

		if page.Error != "" || page.HTML == "" {
			stats.Failed++
			meta.HasHTML = false
			metadata = append(metadata, meta)
			continue
		}

		fileName := generateArchiveFileName(idx+1, page.URL, mainURL)
		meta.FileName = fileName
		meta.HasHTML = true
		meta.SizeBytes = len(page.HTML)

		if err := writeZipEntry(zw, fileName, page.HTML); err != nil {
			_ = zw.Close()
			return "", nil, stats, err
		}

		stats.Successful++
		metadata = append(metadata, meta)
	}

	if err := zw.Close(); err != nil {
		return "", nil, stats, err
	}

	return base64.StdEncoding.EncodeToString(buf.Bytes()), metadata, stats, nil
}

func writeZipEntry(z *zip.Writer, fileName, html string) error {
	w, err := z.Create(fileName)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, strings.NewReader(html))
	return err
}

func generateArchiveFileName(idx int, pageURL, mainURL string) string {
	target := pageURL
	if target == "" {
		target = mainURL
	}

	parsed, err := url.Parse(target)
	if err != nil {
		return fmt.Sprintf("page-%04d.html", idx)
	}

	host := parsed.Hostname()
	path := strings.Trim(parsed.Path, "/")
	if path == "" {
		path = "index"
	}
	base := fmt.Sprintf("%s-%s", host, path)
	base = fileNameSanitizer.ReplaceAllString(base, "-")
	base = strings.Trim(base, "-")
	if base == "" {
		base = "page"
	}
	return fmt.Sprintf("%s-%04d.html", base, idx)
}

