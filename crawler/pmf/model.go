package pmf

import (
	"alpha2/crawler"
)

type Report struct {
	Year          int
	Month         int
	FundManagerID int

	GeneralInfo *crawler.FundManager   `gorm:"-"`
	Services    []DiscretionaryService `gorm:"foreignKey:ReportID"`
	Complaints  *Complaints            `gorm:"foreignKey:ReportID"`
}

type DiscretionaryService struct {
	ID       int
	Strategy string
	FundName string
	AUM      float64

	ReturnsData  map[string]float64 `gorm:"type:jsonb"`
	TurnOverData map[string]float64 `gorm:"type:jsonb"`

	ReportID int
}

type Complaints struct {
	ID int

	PendingMonthStart   float64
	ReceivedDuringMonth float64
	ResolvedDuringMonth float64
	PendingMonthEnd     float64
	ReportID            int
}

func (r *Report) FindServiceByFundName(fundName string) *DiscretionaryService {
	// find the service by fund name, need to implement a fuzzy search
	for _, service := range r.Services {
		if service.FundName == fundName {
			return &service
		}
	}

	// if the name is not found, create a new service
	service := &DiscretionaryService{
		FundName:     fundName,
		ReturnsData:  make(map[string]float64),
		TurnOverData: make(map[string]float64),
	}
	r.Services = append(r.Services, *service)

	return service
}
