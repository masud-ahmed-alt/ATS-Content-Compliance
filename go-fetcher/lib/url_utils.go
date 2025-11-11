package lib

import (
	"net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// NormalizeURL normalizes a URL relative to a base URL
func NormalizeURL(baseURL, href string) string {
	if href == "" || strings.HasPrefix(strings.ToLower(strings.TrimSpace(href)), "javascript:") {
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

// ExtractSameDomainLinks extracts links from HTML that belong to the same domain
func ExtractSameDomainLinks(htmlStr, baseURL string) []string {
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
