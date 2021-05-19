package common

import "time"

type LogMessageData struct {
	System    string      `json:"system"`
	StartTime time.Time   `json:"start_time"`
	Time      time.Time   `json:"time"`
	Level     string      `json:"level"`
	Type      string      `json:"type"`
	Content   interface{} `json:"content"`
}
