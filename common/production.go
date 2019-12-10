package common

type ProductionData struct {
	System       string `json:"system"`        // grapes_gfs_gmf, grapes_gfs_gda
	Type         string `json:"type"`          // grib2
	Event        string `json:"event"`         // storage
	Status       string `json:"status"`        // queue, active, completed, aborted
	StartTime    string `json:"start_time"`    // start time, YYYYMMDDHH
	ForecastTime string `json:"forecast_time"` // time duration, sucha as 3h, 12h, 120h
}
