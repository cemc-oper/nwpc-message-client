package common

type ProductionData struct {
	System       string      `json:"system"`        // grapes_gfs_gmf, grapes_gfs_gda
	Type         string      `json:"type"`          // grib2
	Event        string      `json:"event"`         // storage
	Status       EventStatus `json:"status"`        // unknown, complete, queued, aborted, submitted, active, suspended
	StartTime    string      `json:"start_time"`    // start time, YYYYMMDDHH
	ForecastTime string      `json:"forecast_time"` // time duration, sucha as 3h, 12h, 120h
}
