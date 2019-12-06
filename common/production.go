package common

type ProductionData struct {
	System       string `json:"system"`        // grapes_gfs_gmf
	Type         string `json:"type"`          // grib2
	Event        string `json:"event"`         // storage
	Status       string `json:"status"`        // completed
	StartTime    string `json:"start_time"`    // YYYYMMDDHH
	ForecastTime string `json:"forecast_time"` // 3h
}
