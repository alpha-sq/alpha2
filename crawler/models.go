package crawler

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

type FundManager struct {
	ID uint64

	Name    string
	Email   string
	Contact string

	RegisterNumber string `gorm:"unique"`
	RegisteredDate *time.Time
	Address        string

	TotalNoOfClient *float64
	TotalAUM        *float64

	OtherData JSONB `gorm:"type:jsonb"`

	Funds []*Fund `gorm:"many2many:fund_x_fund_managers" json:"funds"`
}

type Fund struct {
	ID   uint64 `json:"id"`
	Name string `json:"name"`

	FundManagers []*FundManager `gorm:"many2many:fund_x_fund_managers" json:"fund_managers"`
	FundReports  []*FundReport

	Type string `json:"type"`

	Month1Returns  *float64 `json:"1_month_return"`
	Month3Returns  *float64 `json:"3_month_return"`
	Month6Returns  *float64 `json:"6_month_return"`
	Yr1Returns     *float64 `json:"1_year_return"`
	Yr2Returns     *float64 `json:"2_year_return"`
	Yr3Returns     *float64 `json:"3_year_return"`
	Yr4Returns     *float64 `json:"4_year_return"`
	Yr5Returns     *float64 `json:"5_year_return"`
	OverAllReturns *float64 `json:"over_all_return"`
}

type FundReport struct {
	ID     uint64
	FundID uint64 `json:"fund_id"`

	ReportDate *time.Time

	Month1Returns *float64 `json:"1_month_return"`
	Month3Returns *float64 `json:"3_month_return"`
	Month6Returns *float64 `json:"6_month_return"`

	Yr1Returns     *float64 `json:"1_year_return"`
	Yr2Returns     *float64 `json:"2_year_return"`
	Yr3Returns     *float64 `json:"3_year_return"`
	Yr4Returns     *float64 `json:"4_year_return"`
	Yr5Returns     *float64 `json:"5_year_return"`
	OverAllReturns *float64 `json:"over_all_return"`

	OtherData JSONB `gorm:"type:jsonb" json:"-"`
}

type FundXFundManagers struct {
	FundID        uint64 `json:"fund_id"`
	FundManagerID uint64 `json:"fund_manager_id"`
}

type CrawlerEvent struct {
	ID   uint64
	Data JSONB `gorm:"type:jsonb" json:"-"`
}

type JSONB map[string]string

// Scan implements the sql.Scanner interface to read JSONB from the database
func (j *JSONB) Scan(value interface{}) error {
	if value == nil {
		*j = make(JSONB)
		return nil
	}

	bytes, ok := value.([]byte) // PostgreSQL JSONB is returned as []uint8 (bytes)
	if !ok {
		return errors.New("failed to scan JSONB: type assertion to []byte failed")
	}

	return json.Unmarshal(bytes, j)
}

// Value implements the driver.Valuer interface to store JSONB in the database
func (j JSONB) Value() (driver.Value, error) {
	return json.Marshal(j) // Convert Go map to JSON before storing
}
