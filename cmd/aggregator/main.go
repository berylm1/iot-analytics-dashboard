package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/berylm1/iot-analytics-dashboard/internal/models"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker    = "localhost:9092"
	sourceTopic    = "iot-telemetry-raw"
	resultsTopic   = "iot-telemetry-aggregated"
	windowDuration = 1 * time.Minute
)

type WindowAggregator struct {
	temperatures []float64
	humidities   []float64
	deviceIDs    map[string]bool
	windowStart  time.Time
	windowEnd    time.Time
}

func NewWindowAggregator(start time.Time) *WindowAggregator {
	return &WindowAggregator{
		temperatures: make([]float64, 0),
		humidities:   make([]float64, 0),
		deviceIDs:    make(map[string]bool),
		windowStart:  start,
		windowEnd:    start.Add(windowDuration),
	}
}

// Add method to accumulate telemetry data
func (w *WindowAggregator) Add(telemetry models.TelemetryData) {
	w.temperatures = append(w.temperatures, telemetry.Temperature)
	w.humidities = append(w.humidities, telemetry.Humidity)
	w.deviceIDs[telemetry.DeviceID] = true
}

func (w *WindowAggregator) GetAggregation() models.AggregatedData {
	if len(w.temperatures) == 0 {
		return models.AggregatedData{}
	}
	
	var sumTemp, sumHumidity, minTemp, maxTemp float64
	minTemp = w.temperatures[0]
	maxTemp = w.temperatures[0]
	
	for _, temp := range w.temperatures {
		sumTemp += temp
		if temp < minTemp {
			minTemp = temp
		}
		if temp > maxTemp {
			maxTemp = temp
		}
	}
	
	for _, humidity := range w.humidities {
		sumHumidity += humidity
	}
	
	return models.AggregatedData{
		WindowStart:    models.FlinkTimestamp{Time: w.windowStart},
		WindowEnd:      models.FlinkTimestamp{Time: w.windowEnd},
		AvgTemperature: sumTemp / float64(len(w.temperatures)),
		AvgHumidity:    sumHumidity / float64(len(w.humidities)),
		MinTemperature: minTemp,
		MaxTemperature: maxTemp,
		DeviceCount:    len(w.deviceIDs),
	}
}

func main() {
	log.Println("Starting Aggregation Service...")
	
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    sourceTopic,
		GroupID:  "aggregator-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()
	
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    resultsTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		log.Println("Shutting down aggregator...")
		cancel()
	}()
	
	currentWindow := NewWindowAggregator(time.Now().Truncate(windowDuration))
	ticker := time.NewTicker(windowDuration)
	defer ticker.Stop()
	
	log.Printf("Aggregating data in %v windows\n", windowDuration)
	
	msgChan := make(chan models.TelemetryData, 100)
	
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("Error reading message: %v\n", err)
					continue
				}
				
				var telemetry models.TelemetryData
				if err := json.Unmarshal(msg.Value, &telemetry); err != nil {
					log.Printf("Error unmarshaling message: %v\n", err)
					continue
				}
				
				msgChan <- telemetry
			}
		}
	}()
	
	for {
		select {
		case <-ctx.Done():
			log.Println("Aggregator stopped")
			return
		
		case telemetry := <-msgChan:
			currentWindow.Add(telemetry)
		
		case <-ticker.C:
			aggregated := currentWindow.GetAggregation()
			if aggregated.DeviceCount > 0 {
				if err := sendAggregation(ctx, writer, aggregated); err != nil {
					log.Printf("Error sending aggregation: %v\n", err)
				} else {
					log.Printf("Aggregated: Window=%v to %v, AvgTemp=%.2f°C, Devices=%d\n",
						aggregated.WindowStart.Time.Format("15:04:05"),
						aggregated.WindowEnd.Time.Format("15:04:05"),
						aggregated.AvgTemperature,
						aggregated.DeviceCount)
				}
			}
			
			currentWindow = NewWindowAggregator(time.Now().Truncate(windowDuration))
		}
	}
}

func sendAggregation(ctx context.Context, writer *kafka.Writer, aggregated models.AggregatedData) error {
	data, err := json.Marshal(aggregated)
	if err != nil {
		return err
	}
	
	msg := kafka.Message{
		Key:   []byte("aggregated"),
		Value: data,
		Time:  time.Now(),
	}
	
	return writer.WriteMessages(ctx, msg)
}
