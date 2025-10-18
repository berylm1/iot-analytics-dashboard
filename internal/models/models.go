package models

import (
	"encoding/json"
	"time"
)

// TelemetryData represents raw sensor data from IoT devices
type TelemetryData struct {
	DeviceID    string  `json:"deviceId"`
	Temperature float64 `json:"temperature"`
	Humidity    float64 `json:"humidity"`
	Timestamp   time.Time `json:"timestamp"`
	Latitude    float64   `json:"latitude,omitempty"`
	Longitude   float64   `json:"longitude,omitempty"`
}

// FlinkTimestamp is a custom type to handle time serialization
type FlinkTimestamp struct {
	Time time.Time
}

// MarshalJSON customizes how we convert time to JSON
func (ft FlinkTimestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(ft.Time.Format(time.RFC3339))
}

// UnmarshalJSON customizes how we read time from JSON
func (ft *FlinkTimestamp) UnmarshalJSON(data []byte) error {
	var timeStr string
	if err := json.Unmarshal(data, &timeStr); err != nil {
		return err
	}
	parsedTime, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		return err
	}
	ft.Time = parsedTime
	return nil
}

// AggregatedData represents processed data (averages, min, max)
type AggregatedData struct {
	WindowStart    FlinkTimestamp `json:"windowStart"`
	WindowEnd      FlinkTimestamp `json:"windowEnd"`
	AvgTemperature float64        `json:"avgTemperature"`
	AvgHumidity    float64        `json:"avgHumidity"`
	MinTemperature float64        `json:"minTemperature"`
	MaxTemperature float64        `json:"maxTemperature"`
	DeviceCount    int            `json:"deviceCount"`
}
