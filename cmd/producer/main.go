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
	kafkaBroker  = "localhost:9092"
	topic        = "iot-telemetry-raw"
	numDevices   = 5
	sendInterval = 2 * time.Second
)

func main() {
	log.Println("Starting IoT Device Producer...")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down producer...")
		cancel()
	}()

	log.Printf("Simulating %d IoT devices...\n", numDevices)

	// Create device IDs
	devices := make([]string, numDevices)
	for i := 0; i < numDevices; i++ {
		devices[i] = fmt.Sprintf("Device-%d", i+1)
	}

	ticker := time.NewTicker(sendInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Producer stopped")
			return

		case <-ticker.C:
			for _, deviceID := range devices {
				telemetry := generateTelemetry(deviceID)

				if err := sendTelemetry(ctx, writer, telemetry); err != nil {
					log.Printf("Error sending telemetry: %v\n", err)
					continue
				}

				log.Printf("%s -> Temp: %.2f°C, Humidity: %.2f%%\n",
					deviceID, telemetry.Temperature, telemetry.Humidity)
			}
		}
	}
}

func generateTelemetry(deviceID string) models.TelemetryData {
	// Generate random but realistic sensor data
	baseTemp := 20.0 + rand.Float64()*10.0      // 20-30°C
	baseHumidity := 50.0 + rand.Float64()*30.0  // 50-80%

	return models.TelemetryData{
		DeviceID:    deviceID,
		Temperature: baseTemp,
		Humidity:    baseHumidity,
		Timestamp:   time.Now(),
	}
}

func sendTelemetry(ctx context.Context, writer *kafka.Writer, telemetry models.TelemetryData) error {
	data, err := json.Marshal(telemetry)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(telemetry.DeviceID),
		Value: data,
		Time:  time.Now(),
	}

	return writer.WriteMessages(ctx, msg)
}
