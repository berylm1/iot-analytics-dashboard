package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/berylm1/iot-analytics-dashboard/internal/models"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "localhost:9092"
	topic       = "iot-telemetry-raw"
	numDevices  = 10
)

// Device represents an IoT device with location
type Device struct {
	ID        string
	Latitude  float64
	Longitude float64
}

func main() {
	log.Println("Starting IoT Simulator...")

	// Create Kafka writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// Initialize devices with random locations
	devices := initializeDevices(numDevices)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down simulator...")
		cancel()
	}()

	// Start generating telemetry data
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Printf("Simulating %d devices, sending data every 2 seconds\n", numDevices)

	for {
		select {
		case <-ctx.Done():
			log.Println("Simulator stopped")
			return
		case <-ticker.C:
			for _, device := range devices {
				telemetry := generateTelemetry(device)
				if err := sendToKafka(ctx, writer, telemetry); err != nil {
					log.Printf("Error sending telemetry: %v\n", err)
				} else {
					log.Printf("Sent: Device=%s, Temp=%.2f°C, Humidity=%.2f%%\n",
						telemetry.DeviceID, telemetry.Temperature, telemetry.Humidity)
				}
			}
		}
	}
}

func initializeDevices(count int) []Device {
	devices := make([]Device, count)
	// Simulate devices in different locations (San Francisco area)
	baseLatitude := 37.7749
	baseLongitude := -122.4194

	for i := 0; i < count; i++ {
		devices[i] = Device{
			ID:        fmt.Sprintf("device-%03d", i+1),
			Latitude:  baseLatitude + (rand.Float64()-0.5)*0.1,
			Longitude: baseLongitude + (rand.Float64()-0.5)*0.1,
		}
	}
	return devices
}

func generateTelemetry(device Device) models.TelemetryData {
	// Generate realistic sensor data with some variation
	baseTemp := 20.0 + rand.Float64()*15.0  // 20-35°C
	baseHumidity := 40.0 + rand.Float64()*40.0  // 40-80%

	return models.TelemetryData{
		DeviceID:    device.ID,
		Temperature: baseTemp,
		Humidity:    baseHumidity,
		Latitude:    device.Latitude,
		Longitude:   device.Longitude,
		Timestamp:   time.Now().UTC(),
	}
}

func sendToKafka(ctx context.Context, writer *kafka.Writer, telemetry models.TelemetryData) error {
	data, err := json.Marshal(telemetry)
	if err != nil {
		return fmt.Errorf("failed to marshal telemetry: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(telemetry.DeviceID),
		Value: data,
		Time:  time.Now(),
	}

	return writer.WriteMessages(ctx, msg)
}
