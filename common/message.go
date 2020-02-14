package common

import "time"

type EventMessage struct {
	App  string      `json:"app"`  // app name
	Type string      `json:"type"` // type
	Time time.Time   `json:"time"` // time string, RFC 3339
	Data interface{} `json:"data"` // data structure
}
