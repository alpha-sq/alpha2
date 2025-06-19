package api

import (
	"alpha2/crawler"
	"alpha2/crawler/pmf"
	"alpha2/jobs"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/reugn/go-quartz/quartz"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
	"gorm.io/gorm"
)

type FundHouse struct {
	Name         string             `json:"name"`
	ID           string             `json:"id"`
	DisplayName  string             `json:"display_name"`
	LogoUrl      string             `json:"logo_url"`
	Slug         string             `json:"slug"`
	Description  string             `json:"description"`
	Managers     []*crawler.Manager `json:"managers"`
	AUM          *float64           `json:"aum"`
	TotalClients *float64           `json:"total_clients"`
	Strategies   int64              `json:"strategies"`
	LastUpdated  string             `json:"last_updated"`
}

func getFundHouseList(w http.ResponseWriter, r *http.Request) {
	db := crawler.Conn()
	perPage := r.URL.Query().Get("per_page")
	if perPage == "" {
		perPage = "100"
	}
	page := r.URL.Query().Get("page")
	if page == "" {
		page = "1"
	}

	var fundHouseIDs []uint64
	isUnverified := r.URL.Query().Get("unverified") == "true"
	if isUnverified {
		funds := []*crawler.Fund{}
		err := db.Model(&crawler.Fund{}).Where("type = ? and (other_data = 'null' or other_data -> 'label' is null)", "PMF").Preload("FundManagers").Find(&funds).Error
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		lo.ForEach(funds, func(fund *crawler.Fund, _ int) {
			fundHouseIDs = append(fundHouseIDs, fund.FundManagers[0].ID)
		})
	}

	// Convert perPage and page to integers
	perPageInt, err := strconv.Atoi(perPage)
	if err != nil {
		http.Error(w, "Invalid per_page parameter", http.StatusBadRequest)
		return
	}
	pageInt, err := strconv.Atoi(page)
	if err != nil {
		http.Error(w, "Invalid page parameter", http.StatusBadRequest)
		return
	}

	var fundManagers []crawler.FundManager
	tx := db.Find(&crawler.FundManager{}).Limit(perPageInt).Offset((pageInt - 1) * perPageInt)
	if r.URL.Query().Has("id") {
		tx = tx.Where("id = ?", r.URL.Query().Get("id"))
	} else if isUnverified {
		tx = tx.Where("id in ?", fundHouseIDs)
	}
	err = tx.Find(&fundManagers).Error
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fundHouseList := make([]FundHouse, len(fundManagers))
	for i, fundManager := range fundManagers {
		fundHouseList[i] = FundHouse{
			Name:        fundManager.RegistrationName(),
			ID:          strconv.FormatUint(fundManager.ID, 10),
			DisplayName: fundManager.OtherData["display_name"],
			LogoUrl:     fundManager.OtherData["logo_url"],
			Description: fundManager.OtherData["description"],
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(fundHouseList)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getFundHouse(w http.ResponseWriter, r *http.Request) {
	db := crawler.Conn()

	if chi.URLParam(r, "ID") == "" && chi.URLParam(r, "slug") == "" {
		http.Error(w, "ID is required", http.StatusBadRequest)
		return
	}

	var fundManager crawler.FundManager
	tx := db.Model(&crawler.FundManager{}).Preload("Managers")
	if chi.URLParam(r, "ID") != "" {
		tx.Where("id = ?", chi.URLParam(r, "ID"))
	} else {
		tx.Where("other_data->>'slug' = ?", chi.URLParam(r, "slug")).Preload("Funds")
	}
	err := tx.Find(&fundManager).Error
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var lastUpdated string
	if len(fundManager.Funds) != 0 {
		var report crawler.FundReport
		db.Model(&crawler.FundReport{}).
			Order("report_date DESC").
			Where("fund_id in ?", lo.Map(fundManager.Funds, func(fund *crawler.Fund, _ int) uint64 {
				return (fund.ID)
			})).
			First(&report)

		if report.ReportDate != nil {
			lastUpdated = report.ReportDate.Format(time.RFC3339)
		}
	}

	fundHouse := FundHouse{
		Name:         fundManager.RegistrationName(),
		ID:           strconv.FormatUint(fundManager.ID, 10),
		DisplayName:  fundManager.OtherData["display_name"],
		LogoUrl:      fundManager.OtherData["logo_url"],
		Description:  fundManager.OtherData["description"],
		Slug:         fundManager.OtherData["slug"],
		Managers:     fundManager.Managers,
		AUM:          fundManager.TotalAUM,
		TotalClients: fundManager.TotalNoOfClient,
		Strategies:   int64(len(fundManager.Funds)),
		LastUpdated:  lastUpdated,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(fundHouse)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func updateFundHouse(w http.ResponseWriter, r *http.Request) {
	var fundHouse FundHouse
	err := json.NewDecoder(r.Body).Decode(&fundHouse)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	db := crawler.Conn()
	fundManager := crawler.FundManager{}
	err = db.Model(&crawler.FundManager{}).Preload("Managers").First(&fundManager, fundHouse.ID).Error
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	fundManager.OtherData["display_name"] = fundHouse.DisplayName
	if fundHouse.LogoUrl != "" {
		fundManager.OtherData["logo_url"] = fundHouse.LogoUrl
	}
	fundManager.OtherData["description"] = fundHouse.Description
	fundManager.OtherData["slug"] = fundHouse.Slug

	err = db.Transaction(func(tx *gorm.DB) error {
		err = tx.Save(&fundManager).Error
		if err != nil {
			return err
		}

		for _, manager := range fundHouse.Managers {
			if err := tx.Save(manager).Error; err != nil {
				return err
			}
		}

		if fundHouse.LogoUrl != "" {
			err = MarkImageAsUsed(fundHouse.LogoUrl, tx)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	db := crawler.Conn()
	r.ParseMultipartForm(10 << 20) // 10 MB max

	file, header, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Failed to read image: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	contentType := header.Header.Get("Content-Type")
	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "Failed to read file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	image := crawler.Image{
		Filename:    header.Filename,
		Content:     data,
		ContentType: contentType,
		UploadedAt:  time.Now(),
	}

	if err := db.Create(&image).Error; err != nil {
		http.Error(w, "Failed to save image: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "/image?id=%d", image.ID)
}

func getImageHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	var image crawler.Image

	db := crawler.Conn()
	if err := db.First(&image, id).Error; err != nil {
		http.Error(w, "Image not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", image.ContentType)
	w.Write(image.Content)
}

func MarkImageAsUsed(url string, db *gorm.DB) error {
	// get query param from	fundHouse.LogoUrl
	var imgId string
	if strings.Contains(url, "?") {
		imgId = strings.TrimPrefix(url, "/image?id=")
	}

	id, err := strconv.Atoi(imgId)
	if err != nil {
		return fmt.Errorf("invalid image ID: %v", imgId)
	}
	var image crawler.Image
	if err := db.First(&image, id).Error; err != nil {
		return err
	}
	image.IsUnused = false
	return db.Save(&image).Error
}

type Fund struct {
	Name        string  `json:"name"`
	ID          string  `json:"id"`
	DisplayName string  `json:"display_name"`
	IsHidden    bool    `json:"is_hidden"`
	MergedWith  *string `json:"merged_with"`
}

func getFundsListByFundHouse(w http.ResponseWriter, r *http.Request) {
	if chi.URLParam(r, "fund_house_id") == "" {
		http.Error(w, "fund_house_id is required", http.StatusBadRequest)
		return
	}
	fundHouseID := chi.URLParam(r, "fund_house_id")
	db := crawler.Conn()

	fund := &crawler.FundManager{}
	err := db.Model(&crawler.FundManager{}).Where("id = ?", fundHouseID).Preload("Funds").First(fund).Error
	if err != nil {
		http.Error(w, "error during fetching funds", http.StatusBadRequest)
		return
	}

	apiData := make([]Fund, len(fund.Funds))
	for i, fund := range fund.Funds {

		var mergedWith *string
		if fund.OtherData["original_id"] != "" {
			mergedWith = lo.ToPtr(fund.OtherData["original_id"])
			mergeID, err := strconv.ParseUint(*mergedWith, 10, 64)
			if err != nil {
				http.Error(w, "error during parsing merged fund ID", http.StatusBadRequest)
				return
			}

			if mergeID != fund.ID {

				mergedFund := &crawler.Fund{}
				if err = db.Find(&mergedFund, mergeID).Error; err != nil {
					http.Error(w, "error during fetching merged fund", http.StatusBadRequest)
					return
				}

				mergedWith = lo.ToPtr(mergedFund.Name)
			} else {
				mergedWith = nil
			}
		}

		apiData[i] = Fund{
			Name:        fund.Name,
			ID:          strconv.FormatUint(fund.ID, 10),
			DisplayName: fund.OptDisplayName(),
			IsHidden:    fund.IsHidden,
			MergedWith:  mergedWith,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(apiData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func reFetchReport(w http.ResponseWriter, r *http.Request) {
	if chi.URLParam(r, "fund_house_id") == "" {
		log.Error().Msg("fund_house_id is required")
		http.Error(w, "fund_house_id is required", http.StatusBadRequest)
		return
	}
	fundHouseID := chi.URLParam(r, "fund_house_id")
	db := crawler.Conn()

	fundHouse := &crawler.FundManager{}
	err := db.Model(&crawler.FundManager{}).Where("id = ?", fundHouseID).First(fundHouse).Error
	if err != nil {
		log.Error().Err(err).Msg("error during fetching fund house")
		http.Error(w, "error during fetching fund house", http.StatusBadRequest)
		return
	}

	UID := fundHouse.OtherData["UID"]
	data := struct {
		StartDate string `json:"from"`
		EndDate   string `json:"to"`
	}{}
	err = json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	startDate, err := time.Parse(time.RFC3339, data.StartDate)
	if err != nil {

		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	endDate, err := time.Parse(time.RFC3339, data.EndDate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if endDate.After(time.Now()) {
		http.Error(w, "end is future date", http.StatusBadRequest)
		return
	}

	if startDate.Before(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)) {
		http.Error(w, "start date is before 2018", http.StatusBadRequest)
		return
	}

	for startDate.Before(endDate) {
		forDate := time.Date(startDate.Year(), startDate.Month(), 1, 0, 0, 0, 0, time.UTC)

		job := &pmf.CrawlPMFFunds{
			UID:      UID,
			ForDate:  forDate.Format(time.DateOnly),
			SkipNext: true,
		}
		randJobID := lo.RandomString(10, []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"))
		jd := quartz.NewJobDetail(job, quartz.NewJobKeyWithGroup(randJobID, "CrawlPMFFunds"))
		t := quartz.NewRunOnceTrigger(time.Second * 5)
		err = jobs.Scheduler.ScheduleJob(jd, t)
		if err != nil {
			log.Error().Err(err).Msg("Error while refetch scheduling job")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		startDate = time.Date(startDate.Year(), startDate.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	}

	w.WriteHeader(http.StatusOK)
}

func unmergeFund(w http.ResponseWriter, r *http.Request) {

	db := crawler.Conn()

	fundID := chi.URLParam(r, "fund_id")
	if fundID == "" {
		http.Error(w, "fund_id is required", http.StatusBadRequest)
		return
	}

	fundHouseID := chi.URLParam(r, "fund_house_id")
	if fundHouseID == "" {
		http.Error(w, "fund_house_id is required", http.StatusBadRequest)
		return
	}

	err := db.Model(&crawler.FundReport{}).Where("other_data ->> 'merged_id' = ?", fundID).Delete(&crawler.FundReport{}).Error
	if err != nil {
		http.Error(w, "error during deleting reports", http.StatusInternalServerError)
		return
	}

	fund := &crawler.Fund{}
	err = db.Model(&crawler.Fund{}).Where("id = ?", fundID).Find(fund).Error
	if err != nil {
		http.Error(w, "error during fetching fund", http.StatusBadRequest)
		return
	}

	delete(fund.OtherData, "original_id")
	fund.IsHidden = false
	err = db.Save(fund).Error
	if err != nil {
		http.Error(w, "error during saving fund", http.StatusInternalServerError)
		return
	}

	fundHouseIDUint64, err := strconv.ParseUint(fundHouseID, 10, 64)
	if err != nil {
		http.Error(w, "error during parsing fund house ID", http.StatusBadRequest)
		return
	}

	err = pmf.ScheduleDataConsistencyJobIsNotPresent(fundHouseIDUint64)
	if err != nil {
		http.Error(w, "error during scheduling data consistency job", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func mergeFund(w http.ResponseWriter, r *http.Request) {

	db := crawler.Conn()

	fundID := chi.URLParam(r, "fund_id")
	if fundID == "" {
		http.Error(w, "fund_id is required", http.StatusBadRequest)
		return
	}

	mergeFundID := chi.URLParam(r, "merge_fund_id")
	if fundID == "" {
		http.Error(w, "merge_fund_id is required", http.StatusBadRequest)
		return
	}

	fundHouseID := chi.URLParam(r, "fund_house_id")
	if fundHouseID == "" {
		http.Error(w, "fund_house_id is required", http.StatusBadRequest)
		return
	}

	fund := &crawler.Fund{}
	err := db.Model(&crawler.Fund{}).Where("id = ?", fundID).Find(fund).Error
	if err != nil {
		http.Error(w, "error during fetching fund", http.StatusBadRequest)
		return
	}
	mergefund := &crawler.Fund{}
	err = db.Model(&crawler.Fund{}).Where("id = ?", mergeFundID).Find(mergefund).Error
	if err != nil {
		http.Error(w, "error during fetching fund", http.StatusBadRequest)
		return
	}

	fund.OtherData["original_id"] = mergeFundID
	fund.OtherData["label"] = mergefund.OtherData["label"]
	fund.IsHidden = true
	err = db.Save(fund).Error
	if err != nil {
		http.Error(w, "error during saving fund", http.StatusInternalServerError)
		return
	}

	fundHouseIDUint64, err := strconv.ParseUint(fundHouseID, 10, 64)
	if err != nil {
		http.Error(w, "error during parsing fund house ID", http.StatusBadRequest)
		return
	}

	err = pmf.ScheduleDataConsistencyJobIsNotPresent(fundHouseIDUint64)
	if err != nil {
		http.Error(w, "error during scheduling data consistency job", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func hideFund(w http.ResponseWriter, r *http.Request) {

	db := crawler.Conn()

	fundID := chi.URLParam(r, "fund_id")
	if fundID == "" {
		http.Error(w, "fund_id is required", http.StatusBadRequest)
		return
	}

	err := db.Model(&crawler.Fund{}).Where("id = ?", fundID).Update("is_hidden", true).Error
	if err != nil {
		http.Error(w, "error during hiding fund", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func unhideFund(w http.ResponseWriter, r *http.Request) {

	db := crawler.Conn()

	fundID := chi.URLParam(r, "fund_id")
	if fundID == "" {
		http.Error(w, "fund_id is required", http.StatusBadRequest)
		return
	}

	err := db.Model(&crawler.Fund{}).Where("id = ?", fundID).Update("is_hidden", false).Error
	if err != nil {
		http.Error(w, "error during hiding fund", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
