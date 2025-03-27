package api

import (
	"alpha2/crawler"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

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
	if err := db.Where("fund_id = ? AND report_date BETWEEN ? AND ?", fundID, startTime, endTime).Find(&reports).Error; err != nil {
		http.Error(w, "Error fetching reports", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	convertData(reports)
	json.NewEncoder(w).Encode(reports)
}

func convertData(reports []*crawler.FundReport) {
	for idx, report := range reports {
		var c float64
		var b float64
		if report.Month1Returns == nil {
			b = 0
		} else {
			b = *report.Month1Returns
		}
		if idx == 0 {
			c = 100
			r := b + 100
			report.Month1Returns = &r
		} else {
			prvReport := reports[idx-1]
			if prvReport.Month1Returns == nil || report.Month1Returns == nil {
				continue
			}
			c = *prvReport.Month1Returns
			r := (c * b / 100) + c
			report.Month1Returns = &r
		}

	}
}

// Handler to get rolling returns
func getRollingReturns(w http.ResponseWriter, r *http.Request) {
	db := crawler.Conn()
	fundID := chi.URLParam(r, "fundID")
	var reports []crawler.FundReport

	// Fetch last 5 years' reports for rolling calculation
	if err := db.Where("fund_id = ?", fundID).Order("report_date DESC").Limit(5).Find(&reports).Error; err != nil {
		http.Error(w, "Error fetching reports", http.StatusInternalServerError)
		return
	}

	// Calculate rolling average return
	var sumReturns float64
	count := 0
	for _, report := range reports {
		if report.Yr1Returns != nil {
			sumReturns += *report.Yr1Returns
			count++
		}
	}

	rollingReturn := 0.0
	if count > 0 {
		rollingReturn = sumReturns / float64(count)
	}

	resp := map[string]interface{}{
		"fund_id":        fundID,
		"rolling_return": rollingReturn,
		"report_count":   count,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
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
	json.NewEncoder(w).Encode(resp)
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

	tx := db.Model(&crawler.Fund{}).Select("id, name").Preload("FundManagers")
	if fundname != "" {
		tx = tx.Order(clause.OrderBy{
			Expression: clause.Expr{SQL: "similarity(name, ?) DESC", Vars: []any{fundname}},
		})
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
	if err := tx.Find(&funds).Error; err != nil {
		http.Error(w, "Error fetching funds", http.StatusInternalServerError)
		return
	}

	for _, fund := range funds {
		manager := fund.FundManagers[0].OtherData["RegistrationName"]
		apiFunds = append(apiFunds, struct {
			ID      uint64 `json:"id"`
			Name    string `json:"name"`
			Manager string `json:"manager"`
		}{
			ID:      fund.ID,
			Name:    ToTitleCase(fund.Name),
			Manager: manager,
		})
	}

	// Respond with JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(apiFunds)

}

func ToTitleCase(s string) string {
	words := (strings.Trim(s, " ")) // Split string into words
	return strings.ToUpper(string(words[0])) + words[1:]
}
