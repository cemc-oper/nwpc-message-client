package common

type ProductionEvent string

const (
	ProductionEventStorage ProductionEvent = "storage"
)

type ProductionType string

const (
	ProductionTypeObs      ProductionType = "obs"
	ProductionTypeForecast ProductionType = "fcst"
	ProductionTypeGrib2    ProductionType = "grib2"
	ProductionTypeGraph    ProductionType = "graph"
)

type ProductionStream string

const (
	ProductionStreamOperation ProductionStream = "oper"
	ProductionStreamEPS       ProductionStream = "eps"
)

type ProductionInfo struct {
	System  string           `json:"system"`  // system name: grapes_gfs_gmf, grapes_gfs_gda
	Type    ProductionType   `json:"type"`    // production type: grib2
	Stream  ProductionStream `json:"stream"`  // stream: oper, eps, ...
	Product string           `json:"product"` // production name, orig, ...
}

type ProductionEventStatus struct {
	Event  ProductionEvent `json:"event"`  // production event, storage
	Status EventStatus     `json:"status"` // unknown, complete, queued, aborted, submitted, active, suspended
}

type OperationProductionData struct {
	ProductionInfo
	StartTime    string `json:"start_time"`    // start time, YYYYMMDDHH
	ForecastTime string `json:"forecast_time"` // time duration, such as 3h, 12h, 120h
	ProductionEventStatus
}

type EpsProductionData struct {
	ProductionInfo
	StartTime    string `json:"start_time"`    // start time, YYYYMMDDHH
	ForecastTime string `json:"forecast_time"` // time duration, such as 3h, 12h, 120h
	Number       int    `json:"number"`
	ProductionEventStatus
}
