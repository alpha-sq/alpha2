package pmf

import (
	"alpha2/crawler"
	"testing"
	"time"
)

func TestCrawlFund(t *testing.T) {
	crawle := PMFCrawler{}
	forDate, _ := time.Parse("2006-01-02", "2025-01-01")
	crawle.CrawlAllFund(&forDate, func(res []*crawler.Fund) {
		if len(res) == 0 {
			t.Error("CrawlFund should not return empty array")
			return
		}
	})
}
