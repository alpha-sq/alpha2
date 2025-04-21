package api

import (
	"alpha2/crawler"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"gorm.io/gorm"
)

type FundHouse struct {
	Name        string             `json:"name"`
	ID          string             `json:"id"`
	DisplayName string             `json:"display_name"`
	LogoUrl     string             `json:"logo_url"`
	Slug        string             `json:"slug"`
	Description string             `json:"description"`
	Managers    []*crawler.Manager `json:"managers"`
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

	if chi.URLParam(r, "ID") == "" {
		http.Error(w, "ID is required", http.StatusBadRequest)
		return
	}

	var fundManager crawler.FundManager
	err := db.Model(&crawler.FundManager{}).Preload("Managers").Where("id = ?", chi.URLParam(r, "ID")).Find(&fundManager).Error
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fundHouse := FundHouse{
		Name:        fundManager.RegistrationName(),
		ID:          strconv.FormatUint(fundManager.ID, 10),
		DisplayName: fundManager.OtherData["display_name"],
		LogoUrl:     fundManager.OtherData["logo_url"],
		Description: fundManager.OtherData["description"],
		Slug:        fundManager.OtherData["slug"],
		Managers:    fundManager.Managers,
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
