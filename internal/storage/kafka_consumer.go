package storage

import (
	"context"
	"encoding/json"
	"log"

	"github.com/berylm1/iot-analytics-dashboard/internal/models"
	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader *kafka.Reader
	store  Store
}

// NewKafkaConsumer creates a new consumer that reads from Kafka
func NewKafkaConsumer(broker, topic string, store Store) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  "api-consumer-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	return &KafkaConsumer{
		reader: reader,
		store:  store,
	}
}

// Start begins consuming messages from Kafka
func (c *KafkaConsumer) Start(ctx context.Context) {
	log.Println("Starting Kafka consumer for aggregated data...")

	defer c.reader.Close()

	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer stopped")
			return
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Error reading message: %v\n", err)
				continue
			}

			var aggregation models.AggregatedData
			if err := json.Unmarshal(msg.Value, &aggregation); err != nil {
				log.Printf("Error unmarshaling aggregation: %v\n", err)
				continue
			}

			c.store.Add(aggregation)
			log.Printf("Stored aggregation: Window=%v to %v, AvgTemp=%.2f°C\n",
				aggregation.WindowStart.Time.Format("15:04:05"),
				aggregation.WindowEnd.Time.Format("15:04:05"),
				aggregation.AvgTemperature)
		}
	}
}
