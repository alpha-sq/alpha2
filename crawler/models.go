package crawler

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"gorm.io/gorm"
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

	RefreshedDate *time.Time

	OtherData JSONB `gorm:"type:jsonb"`

	Managers []*Manager `json:"managers"`

	Funds []*Fund `gorm:"many2many:fund_x_fund_managers" json:"funds"`
}

type Manager struct {
	ID            uint64 `gorm:"primarykey" json:"id"`
	FundManagerID uint64 `gorm:"<-:create" json:"fund_house"`
	Name          string `json:"name"`
	Title         string `json:"title"`
	About         string `json:"about"`
	Image         string `json:"image"`
	Email         string `json:"email"`
	Contact       string `json:"contact"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

func (f *FundManager) RegistrationName() string {
	return f.OtherData["RegistrationName"]
}

type Fund struct {
	ID   uint64   `json:"id"`
	Name string   `json:"name"`
	AUM  *float64 `json:"aum"`

	IsHidden bool `gorm:"not null;default:false;"`

	FundManagers []*FundManager `gorm:"many2many:fund_x_fund_managers" json:"fund_managers"`
	FundReports  []*FundReport

	Type string `json:"type"`

	Month1Returns *float64 `json:"1_month_return"`
	Month3Returns *float64 `json:"3_month_return"`
	Month6Returns *float64 `json:"6_month_return"`
	Yr1Returns    *float64 `json:"1_year_return"`
	Yr2Returns    *float64 `json:"2_year_return"`
	Yr3Returns    *float64 `json:"3_year_return"`
	Yr4Returns    *float64 `json:"4_year_return"`
	Yr5Returns    *float64 `json:"5_year_return"`

	Yr2Cagr        *float64 `json:"2_year_cagr"`
	Yr3Cagr        *float64 `json:"3_year_cagr"`
	Yr4Cagr        *float64 `json:"4_year_cagr"`
	Yr5Cagr        *float64 `json:"5_year_cagr"`
	OverAllReturns *float64 `json:"over_all_return"`

	MaxDrawdown3Yrs *float64 `json:"max_drawdown_3yr"`
	MaxDrawdown5Yr  *float64 `json:"max_drawdown_5yr"`
	SharpeRatio3Yrs *float64 `json:"sharpe_ratio_3yr"`
	SharpeRatio5Yrs *float64 `json:"sharpe_ratio_5yr"`

	OtherData JSONB `gorm:"type:jsonb"`
}

func (f *Fund) DisplayName() string {
	if f.OtherData == nil {
		return f.Name
	}
	label, ok := f.OtherData["label"]
	if !ok {
		return f.Name
	}
	return label
}
func (f *Fund) OptDisplayName() string {
	if f.OtherData == nil {
		return ""
	}
	label, ok := f.OtherData["label"]
	if !ok {
		return ""
	}
	return label
}

type FundReport struct {
	ID     uint64
	FundID uint64 `json:"fund_id" gorm:"uniqueIndex:idx_report_date_fund_id"`

	ReportDate *time.Time `gorm:"uniqueIndex:idx_report_date_fund_id"`

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

func (f *FundReport) AUM() *float64 {
	if f.OtherData == nil {
		return nil
	}
	aum, ok := f.OtherData["AUM"]
	if !ok {
		return nil
	}
	aumFloat, err := strconv.ParseFloat(aum, 64)
	if err != nil {
		return nil
	}
	return &aumFloat
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

type Image struct {
	ID          uint `gorm:"primaryKey"`
	Filename    string
	Content     []byte `gorm:"type:bytea"` // Important for PostgreSQL
	IsUnused    bool   `gorm:"not null;default:true"`
	ContentType string
	UploadedAt  time.Time
}
