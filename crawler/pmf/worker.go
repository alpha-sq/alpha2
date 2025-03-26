package pmf

import (
	"alpha2/crawler"
	"fmt"
	"net/http"
	"net/url"
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

type PMFCrawler struct {
	fundManagers        []*crawler.FundManager
	fundManagerVsReport *xsync.MapOf[string, []*Report]
	collector           *colly.Collector
	queue               *queue.Queue

	err error
}

func NewPMFCrawler() *PMFCrawler {
	queue, _ := queue.New(
		30,
		nil,
	)
	return &PMFCrawler{
		fundManagers:        make([]*crawler.FundManager, 0),
		fundManagerVsReport: xsync.NewMapOf[string, []*Report](),
		collector:           crawler.NewCollector(),
		queue:               queue,
	}
}

func (p *PMFCrawler) CrawlAllFund(forDate *time.Time, cb crawler.SaveFund) []*crawler.Fund {
	p.registerCrawler(cb)
	p.registerCrawlerError()

	portfolioManagerIDs := CrawlFundManagarIDs()
	for _, uid := range portfolioManagerIDs {
		for i := 1; i <= 12; i++ {
			p.queue.AddRequest(CreateRequest(uid, forDate.Year(), int(forDate.Month())))
		}
	}

	p.queue.Run(p.collector)

	return nil
}

func (p *PMFCrawler) CrawlFundWithManager(UID string, forDate *time.Time, cb crawler.SaveFund) []*crawler.Fund {
	p.registerCrawler(cb)
	p.registerCrawlerError()
	p.queue.AddRequest(CreateRequest(UID, forDate.Year(), int(forDate.Month())))
	p.queue.Run(p.collector)
	return nil
}

func (p *PMFCrawler) ReTryFailed(cb crawler.SaveFund) error {
	db := crawler.Conn()
	var events []crawler.CrawlerEvent
	p.registerCrawler(cb)
	db.Model(&crawler.CrawlerEvent{}).Where("Data->'UID' is not null").FindInBatches(&events, 100, func(tx *gorm.DB, batch int) error {
		bulkQueue, _ := queue.New(
			30,
			nil,
		)
		for _, event := range events {
			UID := event.Data["UID"]
			year, _ := strconv.Atoi(event.Data["year"])
			month, _ := strconv.Atoi(event.Data["month"])
			if err := bulkQueue.AddRequest(CreateRequest(UID, year, month)); err != nil {
				log.Error().Err(err).Msg("Error during AddRequest in ReTryFailed")
			}
		}

		p.collector.OnScraped(func(r *colly.Response) {
			err := tx.Model(&crawler.CrawlerEvent{}).
				Where("data->>'UID' = ? AND data->>'year' = ? AND data->>'month' = ?", r.Ctx.Get("UID"), r.Ctx.Get("year"), r.Ctx.Get("month")).
				Delete(&crawler.CrawlerEvent{}).Error
			if err != nil {
				log.Error().Err(err).Msg("Error during Delete in ReTryFailed")
			}
		})

		p.collector.OnError(func(r *colly.Response, err error) {
			log.Error().Err(err).Msg("Error during colly request in ReTryFailed")
		})

		return bulkQueue.Run(p.collector)
	})
	return nil
}

func (p *PMFCrawler) registerCrawler(cb crawler.SaveFund) {

	//General Information
	p.collector.OnHTML("#main-content", func(e *colly.HTMLElement) {
		report := p.GetReport(e.Request.Ctx)
		e.DOM.Find("strong").EachWithBreak(func(i int, s *goquery.Selection) bool {
			if strings.Contains(s.Text(), "General Information") {
				s.Parent().Parent().Parent().Find("table").Find("tr").Each(func(i int, s *goquery.Selection) {
					switch s.Find("th").Text() {
					case "Name of the Portfolio Manager":
						report.GeneralInfo.OtherData["RegistrationName"] = s.Find("td").Text()
						return
					case "Registration Number":
						report.GeneralInfo.RegisterNumber = s.Find("td").Text()
						return
					case "Date of Registration":
						target, err := time.Parse("2006-01-02", s.Find("td").Text())
						if err != nil {
							log.Error().
								Str("date", s.Find("td").Text()).
								Str("UID", e.Request.Ctx.Get("UID")).
								Err(err).Msg("Error during Date of Registration")
						}
						report.GeneralInfo.RegisteredDate = &target
						return
					case "Registered Address of the Portfolio Manager":
						report.GeneralInfo.Address = s.Find("td").Text()
						return
					case "Name of Principal Officer":
						report.GeneralInfo.Name = s.Find("td").Text()
						return
					case "Email ID of the Principal Officer":
						report.GeneralInfo.Email = s.Find("td").Text()
						return
					case "Contact Number (Direct) of the Principal Officer":
						report.GeneralInfo.Contact = s.Find("td").Text()
						return
					case "Name of Compliance Officer":
						report.GeneralInfo.OtherData["ComplianceOfficer"] = s.Find("td").Text()
						return
					case "Email ID of the Compliance Officer":
						report.GeneralInfo.OtherData["ComplianceOfficerEmail"] = s.Find("td").Text()
						return
					case "No. of clients as on last day of the month":
						tvalue, err := strconv.ParseFloat(s.Find("td").Text(), 64)
						if err != nil {
							log.Error().
								Str("TotalNoOfClient", s.Find("td").Text()).
								Str("UID", e.Request.Ctx.Get("UID")).
								Err(err).Msg("Error during TotalNoOfClient")
						}
						report.GeneralInfo.TotalNoOfClient = &tvalue
						return
					case "Total Assets under Management (AUM) as on last day of the month (Amount in INR crores)":
						tvalue, err := strconv.ParseFloat(s.Find("td").Text(), 64)
						if err != nil {
							log.Error().
								Str("TotalAUM", s.Find("td").Text()).
								Str("UID", e.Request.Ctx.Get("UID")).
								Err(err).Msg("Error during TotalAUM")
						}
						report.GeneralInfo.TotalAUM = &tvalue
						return
					}
				})
				return false
			}
			return true
		})
	})

	//E. Performance Data
	p.collector.OnHTML("#main-content", func(e *colly.HTMLElement) {
		report := p.GetReport(e.Request.Ctx)
		var returnskey []string
		var turnOverkey []string
		singleTableFlow := false

		e.DOM.Find("strong").EachWithBreak(func(i int, s *goquery.Selection) bool {
			if strings.Contains(s.Text(), "E. Performance Data") {
				thead := s.Parent().Parent().Next().Find("thead")

				var returnsKeyLen int64
				var returnSkipKeyLen int64
				var turnOverKeyLen int64
				if len(thead.Children().Nodes) == 2 {
					singleTableFlow = len(thead.Find("tr").First().Children().Nodes) != 2
					thead.Find("tr").First().Find("th").Each(func(i int, s *goquery.Selection) {
						if singleTableFlow {

							if s.Text() == "Returns(%)" {
								returnsKeyLen, _ = strconv.ParseInt(s.AttrOr("colspan", "0"), 10, 64)
							}
							if s.Text() == "Portfolio Turnover Ratio" {
								turnOverKeyLen, _ = strconv.ParseInt(s.AttrOr("colspan", "0"), 10, 64)
							}
						} else {
							if s.Text() == "TWRR Returns (%)" {
								returnsKeyLen, _ = strconv.ParseInt(s.AttrOr("colspan", "0"), 10, 64)
							} else {
								returnSkipKeyLen, _ = strconv.ParseInt(s.AttrOr("colspan", "0"), 10, 64)
							}
						}
					})

					thead.Find("tr").Last().Find("th").Each(func(i int, s *goquery.Selection) {
						if returnSkipKeyLen > 0 {
							returnSkipKeyLen--
							return
						}
						if returnsKeyLen > 0 {
							returnskey = append(returnskey, jsonKey(s.Text()))
							returnsKeyLen--
						}
						if returnsKeyLen == 0 && turnOverKeyLen > 0 {
							turnOverkey = append(turnOverkey, s.Text())
							turnOverKeyLen--
						}
					})
				}

				if !singleTableFlow {
					var fundLen int64
					Strategy := ""
					s.Parent().Parent().Next().Find("tbody").Find("tr").Each(func(i int, s *goquery.Selection) {
						td := s.Children().First()
						if fundLen == 0 {
							Strategy = td.Text()
							fundLen, _ = strconv.ParseInt(td.AttrOr("rowspan", "0"), 10, 64)
							fundLen--
							return
						}
						if fundLen > 0 {
							parseReturnsData(td, Strategy, returnskey, report)
							fundLen--
						}

					})

					turnOverTh := s.Parent().Parent().Next().Next().Find("thead").Find("tr").Last()
					turnOverTh.Children().Each(func(i int, s *goquery.Selection) {
						if s.Text() == "Investment Approach" {
							return
						}

						turnOverkey = append(turnOverkey, s.Text())
					})
					if len(turnOverkey) != 0 {
						s.Parent().Parent().Next().Next().Find("tbody").Find("tr").Each(func(i int, s *goquery.Selection) {
							ds := report.FindServiceByFundName(s.Find("td").First().Text())
							node := s.Find("td").First()

							for _, period := range turnOverkey {
								node = node.Next()
								var err error
								ds.TurnOverData[period], err = strconv.ParseFloat(node.Text(), 64)
								if err != nil {
									fmt.Printf("Error during TurnOverData Convert For fund %s %v\n", ds.FundName, err)
								}
							}

						})
					}
				}

				if singleTableFlow {
					// single table handling
					s.Parent().Parent().Next().Find("tbody").Find("tr").Each(func(i int, s *goquery.Selection) {
						td := s.Children().First()
						Strategy := ""
						ds := parseReturnsData(td, Strategy, returnskey, report)

						if ds != nil {
							for _, period := range turnOverkey {
								td = td.Next()
								var err error
								ds.TurnOverData[period], err = strconv.ParseFloat(td.Text(), 64)
								if err != nil {
									fmt.Printf("Error during TurnOverData convert: %v\n", err)
								}
							}
						}
					})
				}
				return false

			}
			return true
		})
	})

	p.collector.OnHTML("#main-content", func(e *colly.HTMLElement) {
		report := p.GetReport(e.Request.Ctx)
		e.DOM.Find("strong").EachWithBreak(func(i int, s *goquery.Selection) bool {
			if strings.Contains(s.Text(), "Data on Complaints") {
				complaints := report.Complaints
				row := s.Parent().Parent().Next().Find("tbody").Find("tr").Last()
				row.Find("td").Children().Each(func(i int, s *goquery.Selection) {
					switch i {
					case 1:
						complaints.PendingMonthStart, _ = strconv.ParseFloat(s.Text(), 64)
					case 2:
						complaints.ReceivedDuringMonth, _ = strconv.ParseFloat(s.Text(), 64)
					case 3:
						complaints.ResolvedDuringMonth, _ = strconv.ParseFloat(s.Text(), 64)
					case 4:
						complaints.PendingMonthEnd, _ = strconv.ParseFloat(s.Text(), 64)
					}

				})

				return false
			}
			return true
		})
	})

	p.collector.OnScraped(func(r *colly.Response) {
		if val, ok := p.fundManagerVsReport.Load(r.Ctx.Get("UID")); ok {
			cb(reportToFundConverter(val))
		}
		log.Info().Int("Status", r.StatusCode).
			Str("year", r.Ctx.Get("year")).
			Str("month", r.Ctx.Get("month")).
			Str("UID", r.Ctx.Get("UID")).
			Msg("Request scraped successfully")

		p.fundManagerVsReport.Delete(r.Ctx.Get("UID"))
	})

	// p.queue.AddRequest(CreateRequest(UID, forDate.Year(), int(forDate.Month())))

}

func (p *PMFCrawler) registerCrawlerError() {
	p.collector.OnError(func(r *colly.Response, err error) {
		log.Error().Int("Status", r.StatusCode).
			Str("year", r.Ctx.Get("year")).
			Str("month", r.Ctx.Get("month")).
			Str("UID", r.Ctx.Get("UID")).
			Err(err).Msg("Error during colly request")

		p.err = err
		db := crawler.Conn()
		err = db.Create(&crawler.CrawlerEvent{
			Data: map[string]string{
				"year":  r.Ctx.Get("year"),
				"month": r.Ctx.Get("month"),
				"UID":   r.Ctx.Get("UID"),
				"error": err.Error(),
				"time":  time.Now().String(),
			},
		}).Error

		if err != nil {
			log.Error().Err(err).
				Str("year", r.Ctx.Get("year")).
				Str("month", r.Ctx.Get("month")).
				Str("UID", r.Ctx.Get("UID")).
				Err(err).Msg("Create CrawlerEvent Failed")
		}
	})
}

func parseReturnsData(td *goquery.Selection, strategy string, returnskey []string, report *Report) *DiscretionaryService {
	FundName := td.Text()
	//skip the market indices
	if IsIndexName(FundName) || FundName == "0" {
		return nil
	}

	td = td.Next()
	var err error
	if strings.TrimSpace(td.Text()) == "" {
		return nil
	}
	ds := report.FindServiceByFundName(FundName)
	ds.Strategy = strategy
	ds.AUM, err = strconv.ParseFloat(td.Text(), 64)
	if err != nil {
		fmt.Printf("Error during AUM convert for fund %s: %v\n", FundName, err)
	}

	for _, period := range returnskey {
		td = td.Next()
		ds.ReturnsData[period], err = strconv.ParseFloat(td.Text(), 64)
		if err != nil {
			fmt.Printf("Error during ReturnsData convert: %v\n", err)
		}
	}

	return ds
}

func IsIndexName(FundName string) bool {
	indexNames := []string{"NIFTY", "Nifty", "NA", "MIDCAP", "CNXMIDCAP", "GSEC", "SI-BEX", "BSE", "Index", "INDEX", "Benchmark", "Total", "CRISIL", "CLFI", "SENSEX", "MSCIACWI", "CNX100"}
	if FundName == "0" {
		return true
	}
	for _, indexName := range indexNames {
		if strings.Contains(FundName, indexName) {
			return true
		}
	}
	return false
}

func (p *PMFCrawler) GetFundManager(UID string) *crawler.FundManager {
	for _, fm := range p.fundManagers {
		if fm.OtherData["UID"] == UID {
			return fm
		}
	}

	fmm := &crawler.FundManager{}
	err := crawler.Conn().Model(&crawler.FundManager{}).Where("other_data->>'UID' = ?", UID).Preload("Funds").First(fmm).Error
	if err != nil {
		log.Error().Err(err).Str("UID", UID).Msg("Error while fetching FundManager")
	} else if fmm.OtherData != nil && fmm.OtherData["UID"] == UID {
		p.fundManagers = append(p.fundManagers, fmm)
		if fmm.Funds == nil {
			fmm.Funds = make([]*crawler.Fund, 0)
		}
		return fmm
	}

	fm := &crawler.FundManager{
		OtherData: make(map[string]string),
		Funds:     make([]*crawler.Fund, 0),
	}
	fm.OtherData["UID"] = UID

	p.fundManagers = append(p.fundManagers, fm)
	return fm
}

func (p *PMFCrawler) GetReport(ctx *colly.Context) *Report {
	UID := ctx.Get("UID")
	year, _ := strconv.Atoi(ctx.Get("year"))
	month, _ := strconv.Atoi(ctx.Get("month"))
	reports, _ := p.fundManagerVsReport.Load(UID)

	for _, report := range reports {
		if report.Month == month && report.Year == year {
			return report
		}
	}
	report := &Report{
		Year:        year,
		Month:       month,
		GeneralInfo: p.GetFundManager(UID),
		Services:    make([]DiscretionaryService, 0),
		Complaints:  &Complaints{},
	}

	reports = append(reports, report)
	p.fundManagerVsReport.Store(UID, reports)
	return report
}

func CrawlFundManagarIDs() []string {
	c := colly.NewCollector(colly.Debugger(&crawler.LogDebugger{}))
	extensions.RandomUserAgent(c)
	extensions.Referer(c)

	var portfolioManagerIDs []string
	// portfolio managers
	c.OnHTML("div.committee-search > div > select > option", func(e *colly.HTMLElement) {
		// Skip the first option
		if strings.Contains(e.Text, "Select the Portfolio Manager Name") {
			return
		}
		portfolioManagerIDs = append(portfolioManagerIDs, e.Attr("value"))
	})

	c.OnScraped(func(r *colly.Response) {
		log.Info().Int("Status", r.StatusCode).
			Msg("CrawlFundManagarIDs Request scraped successfully")
	})

	c.OnError(func(r *colly.Response, err error) {
		log.Error().Int("Status", r.StatusCode).
			Str("year", r.Ctx.Get("year")).
			Str("month", r.Ctx.Get("month")).
			Str("UID", r.Ctx.Get("UID")).
			Err(err).Msg("Error during colly request")
	})

	c.Visit("https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doPmr=yes")
	c.Wait()
	return portfolioManagerIDs
}

func CreateRequest(UID string, year, month int) *colly.Request {
	url_, _ := url.Parse("https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doPmr=yes")
	// payload := fmt.Sprintf("currdate=&loginflag=0&searchValue=&pmrId=%s&year=%d&month=%d&org.apache.struts.taglib.html.TOKEN=...&loginEmail=&loginPassword=&cap_login=&moduleNo=-1&moduleId=&link=&yourName=&friendName=&friendEmail=&mailmessage=&cap_email=", url.PathEscape(UID), year, month)
	params := url.Values{}
	params.Add("currdate", "")
	params.Add("loginflag", "0")
	params.Add("searchValue", "")
	params.Add("pmrId", UID)
	params.Add("year", strconv.FormatInt(int64(year), 10))
	params.Add("month", strconv.FormatInt(int64(month), 10))
	params.Add("org.apache.struts.taglib.html.TOKEN", "...")
	params.Add("loginEmail", "")
	params.Add("cap_login", "")
	params.Add("moduleNo", "-1")
	params.Add("moduleId", "")
	params.Add("link", "")
	params.Add("yourName", "")
	params.Add("friendName", "")
	params.Add("friendEmail", "")
	params.Add("mailmessage", "")
	params.Add("cap_email", "")
	params.Encode()

	req := &colly.Request{
		URL:     url_,
		Ctx:     colly.NewContext(),
		Method:  "POST",
		Headers: &http.Header{},
		Body:    strings.NewReader(params.Encode()),
	}

	req.Ctx.Put("UID", UID)
	req.Ctx.Put("year", strconv.Itoa(year))
	req.Ctx.Put("month", strconv.Itoa(month))

	req.Headers.Add("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0")
	req.Headers.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Headers.Add("Accept-Language", "en-US,en;q=0.5")
	req.Headers.Add("Accept-Encoding", "gzip, deflate, br, zstd")
	req.Headers.Add("Referer", "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doPmr=yes")
	req.Headers.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Headers.Add("Origin", "https://www.sebi.gov.in")
	req.Headers.Add("Connection", "keep-alive")
	req.Headers.Add("Upgrade-Insecure-Requests", "1")
	req.Headers.Add("Sec-Fetch-Dest", "document")
	req.Headers.Add("Sec-Fetch-Mode", "navigate")
	req.Headers.Add("Sec-Fetch-Site", "same-origin")
	req.Headers.Add("Sec-Fetch-User", "?1")
	req.Headers.Add("Priority", "u=0, i")
	req.Headers.Add("Cookie", "JSESSIONID=BA0846513788A51696A1C56A5F37A38E")

	return req
}

func reportToFundConverter(reports []*Report) []*crawler.Fund {
	funds := make([]*crawler.Fund, 0)

	for _, report := range reports {
		for _, service := range report.Services {

			reportDate, _ := time.Parse("2006-01-02", fmt.Sprintf("%04d-%02d-01", report.Year, report.Month))
			fundReport := &crawler.FundReport{
				ReportDate: &reportDate,
				OtherData:  make(map[string]string),
			}
			if _, ok := service.ReturnsData["1 month"]; ok {
				temp := service.ReturnsData["1 month"]
				fundReport.Month1Returns = &temp
			}
			if _, ok := service.ReturnsData["3 month"]; ok {
				temp := service.ReturnsData["3 month"]
				fundReport.Month3Returns = &temp
			}
			if _, ok := service.ReturnsData["6 month"]; ok {
				temp := service.ReturnsData["6 month"]
				fundReport.Month6Returns = &temp
			}
			if _, ok := service.ReturnsData["1 year"]; ok {
				temp := service.ReturnsData["1 year"]
				fundReport.Yr1Returns = &temp
			}
			if _, ok := service.ReturnsData["2 year"]; ok {
				temp := service.ReturnsData["2 year"]
				fundReport.Yr2Returns = &temp
			}
			if _, ok := service.ReturnsData["3 year"]; ok {
				temp := service.ReturnsData["3 year"]
				fundReport.Yr3Returns = &temp
			}
			if _, ok := service.ReturnsData["4 year"]; ok {
				temp := service.ReturnsData["4 year"]
				fundReport.Yr4Returns = &temp
			}
			if _, ok := service.ReturnsData["5 year"]; ok {
				temp := service.ReturnsData["5 year"]
				fundReport.Yr5Returns = &temp
			}

			if _, ok := service.ReturnsData["since inception"]; ok {
				temp := service.ReturnsData["since inception"]
				fundReport.OverAllReturns = &temp
			}

			if service.Strategy == "" {
				service.Strategy = "Equity"
			}
			fundReport.OtherData["Strategy"] = service.Strategy

			fund := &crawler.Fund{
				ID:           service.ID,
				Name:         service.FundName,
				Type:         "PMF",
				FundManagers: []*crawler.FundManager{report.GeneralInfo},
				FundReports:  []*crawler.FundReport{fundReport},
			}

			funds = append(funds, fund)
		}
	}

	return funds
}

func jsonKey(k string) string {
	k = strings.ToLower(strings.TrimSpace(k))
	k = strings.ReplaceAll(k, "years", "year")
	k = strings.ReplaceAll(k, "months", "month")
	return k
}
