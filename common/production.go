package common

type ProductionEvent string

const (
	ProductionEventStorage ProductionEvent = "storage"
)

type ProductionType string

const (
	ProductionTypeGrib2 ProductionType = "grib2"
)

type ProductionData struct {
	System       string          `json:"system"`        // grapes_gfs_gmf, grapes_gfs_gda
	Type         ProductionType  `json:"type"`          // grib2
	Event        ProductionEvent `json:"event"`         // storage
	Status       EventStatus     `json:"status"`        // unknown, complete, queued, aborted, submitted, active, suspended
	StartTime    string          `json:"start_time"`    // start time, YYYYMMDDHH
	ForecastTime string          `json:"forecast_time"` // time duration, such as 3h, 12h, 120h
}
