package api

import (
	"alpha2/crawler"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
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

	var reports []crawler.FundReport
	if err := db.Where("fund_id = ? AND report_date BETWEEN ? AND ?", fundID, startTime, endTime).Find(&reports).Error; err != nil {
		http.Error(w, "Error fetching reports", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reports)
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

	// Fetch fund reports
	if err := db.Where("fund_id = ?", fundID).Order("report_date DESC").Find(&reports).Error; err != nil {
		http.Error(w, "Error fetching reports", http.StatusInternalServerError)
		return
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
	var funds []struct {
		ID   uint64 `json:"id"`
		Name string `json:"name"`
	}

	// Select only ID and Name from Fund table
	if err := db.Model(&crawler.Fund{}).Select("id, name").Find(&funds).Error; err != nil {
		http.Error(w, "Error fetching funds", http.StatusInternalServerError)
		return
	}

	// Respond with JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(funds)

}
