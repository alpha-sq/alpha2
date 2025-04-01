package mf

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/extensions"
	"github.com/gocolly/colly/v2/queue"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
)

type MutualFundData struct {
	gorm.Model
	Name   string `json:"schemename" gorm:"unique"`
	NavURl string

	Navs []*MutualFundNav `json:"navs" gorm:"foreignKey:MutualFundDataID"`
}

type MutualFundNav struct {
	gorm.Model
	MutualFundDataID uint
	Nav              *float64   `json:"navrs"`
	Date             *time.Time `json:"navdate"`
}

type MutualFundCrawler struct {
	collector *colly.Collector
	queue     *queue.Queue
}

func NewMutualFundCrawler() *MutualFundCrawler {
	collector := colly.NewCollector()
	extensions.RandomUserAgent(collector)
	extensions.Referer(collector)
	queue, _ := queue.New(
		2,
		&queue.InMemoryQueueStorage{MaxSize: 10000},
	)

	return &MutualFundCrawler{
		collector: collector,
		queue:     queue,
	}
}

func (mfc *MutualFundCrawler) CrawlFundMeta() (funds []*MutualFundData, err error) {
	reqURL, _ := url.Parse("https://www.advisorkhoj.com/mutual-funds-research/mutual-fund-latest-nav")
	funds = make([]*MutualFundData, 0)
	req := &colly.Request{
		URL:     reqURL,
		Ctx:     colly.NewContext(),
		Method:  "GET",
		Headers: &http.Header{},
	}
	addHeaders(req)

	mfc.queue.AddRequest(req)

	m := xsync.NewMapOf[string, *MutualFundData]()
	mfc.collector.OnHTML("table#latest_nav", func(e *colly.HTMLElement) {
		e.DOM.Find("tbody tr").Each(func(i int, s *goquery.Selection) {
			name := s.Find("td").First().Find("a").Text()
			navUrl := s.Find("td").Last().Find("a").AttrOr("href", "")

			fund := &MutualFundData{
				Name:   name,
				NavURl: navUrl,
			}
			m.Store(name, fund)
		})
	})

	mfc.collector.OnScraped(func(r *colly.Response) {
		m.Range(func(k string, v *MutualFundData) bool {
			funds = append(funds, v)
			return true
		})
	})

	mfc.collector.OnError(func(r *colly.Response, err_ error) {
		err = err_
		log.Error().Err(err).Msg("Error occurred while crawling mutual fund data")
	})

	mfc.queue.Run(mfc.collector)
	return
}
func (mfc *MutualFundCrawler) CrawlFundNav(fund *MutualFundData) (navs []*MutualFundNav, err error) {
	if !strings.HasPrefix(fund.NavURl, "/") {
		fund.NavURl = fmt.Sprintf("/%s", fund.NavURl)
	}

	navs = make([]*MutualFundNav, 0)
	endDate := time.Now().AddDate(0, -1, 0).Format("02-01-2006")
	startDate := time.Now().AddDate(-10, 1, 0).Format("02-01-2006")
	reqURL, _ := url.Parse(fmt.Sprintf("https://www.advisorkhoj.com%s?end_date=%s&start_date=%s", fund.NavURl, endDate, startDate))
	// reqURL, _ := url.Parse("https://www.advisorkhoj.com/mutual-funds-research/historical-NAV/HDFC%20Arbitrage%20Dir%20Gr?start_date=07-04-2015&end_date=13-03-2025")
	tfil, err := wgetFile(reqURL.String())
	if err != nil {
		return nil, err
	}

	defer os.Remove(tfil.Name())

	ct, err := os.ReadFile(tfil.Name())
	if err != nil {
		return nil, err
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(ct)))
	if err != nil {
		return nil, err
	}

	doc.Find("table#historical_nav").Find("tbody tr").
		Each(func(i int, s *goquery.Selection) {
			dateStr := s.Find("td").First().Text()
			navStr := s.Find("td").Last().Text()

			date, _ := time.Parse("02-01-2006", dateStr)
			nav, _ := strconv.ParseFloat(navStr, 64)
			fundNav := &MutualFundNav{
				MutualFundDataID: fund.ID,
				Nav:              &nav,
				Date:             &date,
			}
			navs = append(navs, fundNav)
		})

	return
}

func wgetFile(reqUrl string) (*os.File, error) {
	tmpFile, err := os.CreateTemp("", "prefix_*.txt")
	cmd := exec.Command("wget", "-O", tmpFile.Name(), reqUrl)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()
	return tmpFile, err
}

func addHeaders(req *colly.Request) {
	req.Headers.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Headers.Add("Accept-Language", "en-US,en;q=0.5")
	req.Headers.Add("Accept-Encoding", "gzip, deflate, br, zstd")
	req.Headers.Add("Referer", "https://www.advisorkhoj.com/mutual-funds-research/mutual-fund-portfolio/Axis-Mutual-Fund/2024")
	req.Headers.Add("Connection", "keep-alive")
	req.Headers.Add("Cookie", "_GPSLSC=; JSESSIONID=7C8F93A86A4B5932931D550CAFFF596B; G_ENABLED_IDPS=google; dsq__u=298eo8n38cmibc; dsq__s=298eo8n38cmibc; _GPSLSC=")
	req.Headers.Add("Upgrade-Insecure-Requests", "1")
	req.Headers.Add("Sec-Fetch-Dest", "document")
	req.Headers.Add("Sec-Fetch-Mode", "navigate")
	req.Headers.Add("Sec-Fetch-Site", "same-origin")
	req.Headers.Add("Priority", "u=0, i")
}

func addHeaders2(req *colly.Request) {
	req.Headers.Add("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:136.0) Gecko/20100101 Firefox/136.0")
	req.Headers.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Headers.Add("Accept-Language", "en-US,en;q=0.5")
	req.Headers.Add("Accept-Encoding", "gzip, deflate, br, zstd")
	req.Headers.Add("Connection", "keep-alive")
	req.Headers.Add("Referer", "https://www.advisorkhoj.com/mutual-funds-research/historical-NAV/HDFC%20Overnight%20Gr?start_date=07-04-2015&end_date=13-11-2019")
	req.Headers.Add("Cookie", "_GPSLSC=; _GPSLSC=; JSESSIONID=115879A3A0BBC5DC6EE5AC68917995FB; G_ENABLED_IDPS=google; dsq__u=29chejv1vvckv4; dsq__s=29chejv1vvckv4; _GPSLSC=")
	req.Headers.Add("Upgrade-Insecure-Requests", "1")
	req.Headers.Add("Sec-Fetch-Dest", "document")
	req.Headers.Add("Sec-Fetch-Mode", "navigate")
	req.Headers.Add("Sec-Fetch-Site", "same-origin")
	req.Headers.Add("Sec-Fetch-User", "?1")
	req.Headers.Add("Priority", "u=0, i")

}
