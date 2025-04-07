package api

import (
	"alpha2/crawler"
	"encoding/csv"
	"encoding/json"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type LineGraphData struct {
	ReportDate *time.Time `json:"report_date"`
	Amount     *float64   `json:"amount"`
	Returns    *float64   `json:"returns"`
}

// Handler to get trailing returns
func getTrailingReturns(w http.ResponseWriter, r *http.Request) {
	db := crawler.Conn()
	fundIDStr := chi.URLParam(r, "fundID")
	fundID, err := strconv.ParseUint(fundIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid fund ID", http.StatusBadRequest)
		return
	}

	// Parse start and end times from query parameters
	startTimeStr := r.URL.Query().Get("start")
	endTimeStr := r.URL.Query().Get("end")

	// Validate start and end times
	if startTimeStr == "" || endTimeStr == "" {
		http.Error(w, "Both start and end times are required", http.StatusBadRequest)
		return
	}

	// Parse start and end times into time.Time
	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		http.Error(w, "Invalid start time format (use RFC3339, e.g., 2023-10-01T00:00:00Z)", http.StatusBadRequest)
		return
	}

	endTime, err := time.Parse(time.RFC3339, endTimeStr)
	if err != nil {
		http.Error(w, "Invalid end time format (use RFC3339, e.g., 2023-10-31T23:59:59Z)", http.StatusBadRequest)
		return
	}

	var reports []*crawler.FundReport
	err = db.
		Raw(`
        SELECT DISTINCT ON (fund_id, DATE_TRUNC('month', report_date)) *
        FROM fund_reports
        WHERE fund_id = ?
          AND report_date BETWEEN ? AND ?
        ORDER BY fund_id, DATE_TRUNC('month', report_date), report_date DESC
    `, fundID, startTime, endTime).
		Scan(&reports).Error
	if err != nil {
		http.Error(w, "Error fetching reports", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	apiData := convertData(reports)
	if err := json.NewEncoder(w).Encode(apiData); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}

func convertData(reports []*crawler.FundReport) []*LineGraphData {
	apiData := make([]*LineGraphData, 0)
	for idx, report := range reports {
		var c float64
		var b float64
		data := LineGraphData{
			ReportDate: report.ReportDate,
			Returns:    report.Month1Returns,
		}
		if report.Month1Returns == nil {
			b = 0
		} else {
			b = *report.Month1Returns
		}
		if idx == 0 {
			c = 100
			// r := b + 100
			report.Month1Returns = &c
		} else {
			prvReport := reports[idx-1]
			if prvReport.Month1Returns == nil || report.Month1Returns == nil {
				continue
			}
			c = *prvReport.Month1Returns
			r := (c * b / 100) + c
			report.Month1Returns = &r
		}

		data.Amount = report.Month1Returns
		apiData = append(apiData, &data)
	}

	return apiData
}

// Handler to get discrete returns
func getDiscreteReturns(w http.ResponseWriter, r *http.Request) {
	db := crawler.Conn()
	fundID := chi.URLParam(r, "fundID")
	var reports []crawler.FundReport

	period := r.URL.Query().Get("period")
	if period == "Y" {
		month := time.Now().AddDate(0, -1, 0).Month()
		err := db.Raw(`
        	SELECT DISTINCT ON (EXTRACT(YEAR FROM report_date)) *
        	FROM fund_reports
        	WHERE EXTRACT(MONTH FROM report_date) = ? AND fund_id = ?
        	ORDER BY EXTRACT(YEAR FROM report_date), report_date DESC;
    	`, month, fundID).Scan(&reports).Error
		if err != nil {
			http.Error(w, "Error fetching reports", http.StatusInternalServerError)
			return
		}
	} else if period == "Q" {

		limit := 50
		now := time.Now()
		currentYear, currentMonth, _ := now.Date()

		// Calculate the end date (start of current month to exclude current month)
		endDate := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, time.UTC)
		quarterCount := 0
		for qStart := endDate.AddDate(0, -3, 0); quarterCount < limit; qStart = qStart.AddDate(0, -3, 0) {
			qEnd := qStart.AddDate(0, 3, 0)

			var report crawler.FundReport
			err := db.
				Where("fund_id = ?", fundID).
				Where("report_date >= ? AND report_date < ?", qStart, qEnd).
				Order("report_date DESC").
				First(&report).Error

			if err != nil && err != gorm.ErrRecordNotFound {
				http.Error(w, "Error fetching reports", http.StatusInternalServerError)
				return
			}

			if err == nil {
				reports = append(reports, report)
				quarterCount++
			}

			// Stop if we've gone back too far (optional)
			if qStart.Year() < now.Year()-5 { // 5 year limit
				break
			}
		}
	}

	// Prepare discrete return response
	var discreteReturns []map[string]interface{}
	for _, report := range reports {
		discreteReturns = append(discreteReturns, map[string]interface{}{
			"report_date": report.ReportDate,
			"1yr_return":  report.Yr1Returns,
			"2yr_return":  report.Yr2Returns,
			"3yr_return":  report.Yr3Returns,
			"4yr_return":  report.Yr4Returns,
			"5yr_return":  report.Yr5Returns,
		})
	}

	resp := map[string]interface{}{
		"fund_id":          fundID,
		"discrete_returns": discreteReturns,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

}

func getAllFunds(w http.ResponseWriter, r *http.Request) {
	db := crawler.Conn()
	var apiFunds []struct {
		ID      uint64 `json:"id"`
		Name    string `json:"name"`
		Manager string `json:"manager"`
	}

	fundname := r.URL.Query().Get("name")
	perPage := r.URL.Query().Get("per_page")
	ftype := r.URL.Query().Get("type")
	if ftype == "" {
		ftype = "PMF"
	}

	var tx *gorm.DB
	if ftype == "PMF" {
		tx = db.Model(&crawler.Fund{}).Preload("FundManagers").Where("type = 'PMF' and  other_data != 'null' and is_hidden = false")
		if fundname != "" {
			tx = tx.Order(clause.OrderBy{
				Expression: clause.Expr{SQL: "similarity(other_data->>'label', ?) DESC", Vars: []any{fundname}},
			})
		}
	} else {
		tx = db.Model(&crawler.Fund{}).Where("type = 'MF' and is_hidden = false")
		if fundname != "" {
			tx = tx.Order(clause.OrderBy{
				Expression: clause.Expr{SQL: "similarity(name, ?) DESC", Vars: []any{fundname}},
			})
		}
	}

	if perPage == "" {
		perPage = "50"
	}
	perPageInt, err := strconv.Atoi(perPage)
	if err != nil {
		http.Error(w, "Invalid per_page value", http.StatusBadRequest)
		return
	}
	tx = tx.Limit(perPageInt)
	// Select only ID and Name from Fund table
	funds := []crawler.Fund{}
	if err := tx.
		Find(&funds).Error; err != nil {
		http.Error(w, "Error fetching funds", http.StatusInternalServerError)
		return
	}

	for _, fund := range funds {
		var manager string
		if len(fund.FundManagers) > 0 {
			manager = fund.FundManagers[0].OtherData["RegistrationName"]
		} else {
			manager = "Mutual Fund"
		}
		apiFunds = append(apiFunds, struct {
			ID      uint64 `json:"id"`
			Name    string `json:"name"`
			Manager string `json:"manager"`
		}{
			ID:      fund.ID,
			Name:    ToTitleCase(fund.DisplayName()),
			Manager: manager,
		})
	}

	// Respond with JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(apiFunds); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

}

func ToTitleCase(s string) string {
	s = strings.ReplaceAll(s, "\n", "")

	words := (strings.Trim(s, " ")) // Split string into words
	if len(words) == 0 {
		return ""
	}
	return strings.ToUpper(string(words[0])) + words[1:]
}

func getExplorePMSData(w http.ResponseWriter, r *http.Request) {
	db := crawler.Conn()
	var resp struct {
		Data []struct {
			ID      uint64   `json:"id"`
			Name    string   `json:"schemeName"`
			Manager string   `json:"manager"`
			AUM     *float64 `json:"aum"`

			OneMonth    *float64 `json:"oneMonth"`
			ThreeMonth  *float64 `json:"threeMonth"`
			SixMonth    *float64 `json:"sixMonth"`
			OneYear     *float64 `json:"oneYear"`
			TwoYear     *float64 `json:"twoYear"`
			ThreeYear   *float64 `json:"threeYear"`
			FiveYear    *float64 `json:"fiveYear"`
			YTD         *float64 `json:"ytd"`
			SharpeRatio *float64 `json:"sharpeRatio"`
			MaxDrawdown *float64 `json:"maxDrawdown"`
		} `json:"data"`

		Total int64 `json:"total"`
	}

	fundname := r.URL.Query().Get("search")
	perPage := r.URL.Query().Get("per_page")
	page := r.URL.Query().Get("page")

	orderby := r.URL.Query().Get("order_by")
	if orderby == "" {
		orderby = "name"
	}
	order := r.URL.Query().Get("order")
	isDesc := false
	if strings.ToLower(order) == "desc" {
		isDesc = true
	}

	tx := db.Model(&crawler.FundReport{})

	if perPage == "" {
		perPage = "50"
	}
	if page == "" {
		page = "0"
	}
	perPageInt, err := strconv.Atoi(perPage)
	if err != nil {
		http.Error(w, "Invalid per_page value", http.StatusBadRequest)
		return
	}
	pageInt, err := strconv.Atoi(page)
	if err != nil {
		http.Error(w, "Invalid per_page value", http.StatusBadRequest)
		return
	}
	// Select only ID and Name from Fund table
	now := time.Now()
	firstDayLastMonth := time.Date(now.Year(), now.Month()-2, 1, 0, 0, 0, 0, now.Location())
	lastDayLastMonth := firstDayLastMonth.AddDate(0, 1, -1)

	tx = tx.Joins("JOIN funds ON funds.id = fund_reports.fund_id").
		Where("report_date BETWEEN ? AND ?", firstDayLastMonth, lastDayLastMonth).
		Where("funds.name != '' and  funds.type = 'PMF' and funds.is_hidden = false")
	filter := r.URL.Query().Get("filter")
	if filter != "" && filter != "All Funds" {
		tx.Where("funds.other_data != 'null' and funds.other_data->>'label' in ?", getFundsByFilter(filter))
	}
	if fundname != "" {
		tx.Where("similarity(funds.name, ?) > 0.1", fundname)
	}

	switch orderby {
	case "aum":
		tx = tx.Order(clause.OrderBy{
			Columns: []clause.OrderByColumn{{
				Column: clause.Column{Name: "aum"},
				Desc:   isDesc,
			}},
		})
	case "threeMonth":
		tx = tx.Order(clause.OrderBy{
			Columns: []clause.OrderByColumn{{
				Column: clause.Column{Name: "fund_reports.month3_returns"},
				Desc:   isDesc,
			}},
		})
		tx = tx.Where("fund_reports.month3_returns IS NOT NULL")
	case "sixMonth":
		tx = tx.Order(clause.OrderBy{
			Columns: []clause.OrderByColumn{{
				Column: clause.Column{Name: "fund_reports.month6_returns"},
				Desc:   isDesc,
			}},
		})
		tx = tx.Where("fund_reports.month6_returns IS NOT NULL")
	case "oneYear":
		tx = tx.Order(clause.OrderBy{
			Columns: []clause.OrderByColumn{{
				Column: clause.Column{Name: "fund_reports.yr1_returns"},
				Desc:   isDesc,
			}},
		})
		tx = tx.Where("fund_reports.yr1_returns IS NOT NULL")
	case "twoYear":
		tx = tx.Order(clause.OrderBy{
			Columns: []clause.OrderByColumn{{
				Column: clause.Column{Name: "fund_reports.yr2_returns"},
				Desc:   isDesc,
			}},
		})
		tx = tx.Where("fund_reports.yr2_returns IS NOT NULL")
	case "threeYear":
		tx = tx.Order(clause.OrderBy{
			Columns: []clause.OrderByColumn{{
				Column: clause.Column{Name: "fund_reports.yr3_returns"},
				Desc:   isDesc,
			}},
		})
		tx = tx.Where("fund_reports.yr3_returns IS NOT NULL")
	case "fiveYear":
		tx = tx.Order(clause.OrderBy{
			Columns: []clause.OrderByColumn{{
				Column: clause.Column{Name: "fund_reports.yr5_returns"},
				Desc:   isDesc,
			}},
		})
		tx = tx.Where("fund_reports.yr5_returns IS NOT NULL")
	case "ytd":
		tx = tx.Order(clause.OrderBy{
			Columns: []clause.OrderByColumn{{
				Column: clause.Column{Name: "fund_reports.over_all_returns"},
				Desc:   isDesc,
			}},
		})
		tx = tx.Where("fund_reports.over_all_returns IS NOT NULL")
	case "sharpeRatio":
		tx = tx.Order(clause.OrderBy{
			Columns: []clause.OrderByColumn{{
				Column: clause.Column{Name: "funds.sharpe_ratio3_yrs"},
				Desc:   isDesc,
			}},
		})
		tx = tx.Where("funds.sharpe_ratio3_yrs IS NOT NULL")
	case "maxDrawdown":
		tx = tx.Order(clause.OrderBy{
			Columns: []clause.OrderByColumn{{
				Column: clause.Column{Name: "funds.max_drawdown3_yrs"},
				Desc:   isDesc,
			}},
		})
		tx = tx.Where("funds.max_drawdown3_yrs IS NOT NULL")
	default:
		tx = tx.Order(clause.OrderBy{
			Columns: []clause.OrderByColumn{{
				Column: clause.Column{Name: "funds.name"},
				Desc:   isDesc,
			}},
		})

	}

	reports := []crawler.FundReport{}
	if err := tx.Session(&gorm.Session{}).Limit(perPageInt).Offset(pageInt).Find(&reports).Error; err != nil {
		http.Error(w, "Error fetching reports", http.StatusInternalServerError)
		return
	}

	funds := []crawler.Fund{}
	fundIDs := make([]uint64, len(reports))
	for i, report := range reports {
		fundIDs[i] = report.FundID
	}
	if err := db.Model(&crawler.Fund{}).Where("id in ?", fundIDs).Preload("FundManagers").Find(&funds).Error; err != nil {
		http.Error(w, "Error fetching funds", http.StatusInternalServerError)
		return
	}

	if err := tx.Count(&resp.Total).Error; err != nil {
		http.Error(w, "Error fetching funds", http.StatusInternalServerError)
		return
	}

	for _, fund := range funds {
		var report *crawler.FundReport
		for _, r := range reports {
			if fund.ID == r.FundID {
				report = &r
				break
			}
		}
		if report == nil || report.AUM() == nil || *report.AUM() == 0 {
			continue
		}

		if Round(report.Month1Returns) == nil && report.Month3Returns == nil && report.Month6Returns == nil && report.Yr1Returns == nil {
			continue
		}

		if (Round(report.Month1Returns) != nil && *Round(report.Month1Returns) == 0) && (Round(report.Month3Returns) != nil && *Round(report.Month3Returns) == 0) && (Round(report.Month6Returns) != nil && *Round(report.Month6Returns) == 0) {
			continue
		}

		var manager string
		if len(fund.FundManagers) != 0 {
			manager = ToTitleCase(fund.FundManagers[0].RegistrationName())
		}

		resp.Data = append(resp.Data, struct {
			ID          uint64   "json:\"id\""
			Name        string   "json:\"schemeName\""
			Manager     string   `json:"manager"`
			AUM         *float64 "json:\"aum\""
			OneMonth    *float64 `json:"oneMonth"`
			ThreeMonth  *float64 "json:\"threeMonth\""
			SixMonth    *float64 "json:\"sixMonth\""
			OneYear     *float64 "json:\"oneYear\""
			TwoYear     *float64 "json:\"twoYear\""
			ThreeYear   *float64 "json:\"threeYear\""
			FiveYear    *float64 "json:\"fiveYear\""
			YTD         *float64 "json:\"ytd\""
			SharpeRatio *float64 "json:\"sharpeRatio\""
			MaxDrawdown *float64 "json:\"maxDrawdown\""
		}{
			ID:          fund.ID,
			Name:        ToTitleCase(fund.DisplayName()),
			Manager:     manager,
			AUM:         Round(report.AUM()),
			OneMonth:    Round(report.Month1Returns),
			ThreeMonth:  Round(report.Month3Returns),
			SixMonth:    Round(report.Month6Returns),
			OneYear:     Round(report.Yr1Returns),
			TwoYear:     Round(report.Yr2Returns),
			ThreeYear:   Round(report.Yr3Returns),
			FiveYear:    Round(report.Yr5Returns),
			YTD:         Round(report.OverAllReturns),
			SharpeRatio: Round(fund.SharpeRatio3Yrs),
			MaxDrawdown: Round(fund.MaxDrawdown3Yrs),
		})
	}

	// Respond with JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}

func Round(num *float64) *float64 {
	if num == nil || math.IsNaN(*num) {
		return nil
	}

	t := math.Round(*num*100) / 100
	return &t

}

func getFundsByFilter(filter string) []string {
	file, err := os.Open("./static/filters.csv")
	if err != nil {
		log.Error().Err(err).Msg("Error opening file")
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		log.Error().Err(err).Msg("Error opening file")
	}

	funds := make([]string, 0)
	lo.ForEach(records, func(rec []string, i int) {
		if len(rec) < 2 {
			return
		}
		if rec[1] == filter {
			funds = append(funds, rec[0])
		}
	})

	return funds
}
