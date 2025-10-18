// File: backend/main.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"github.com/segmentio/kafka-go"
)

// ====================
// PART 1: DATA STRUCTURES
// ====================

// DeviceMetrics represents the aggregated metrics from Flink
type DeviceMetrics struct {
	Timestamp      string  `json:"timestamp"`
	WindowStart    string  `json:"windowStart"`
	WindowEnd      string  `json:"windowEnd"`
	AvgTemperature float64 `json:"avgTemperature"`
	AvgHumidity    float64 `json:"avgHumidity"`
	DeviceCount    int     `json:"deviceCount"`
}

// MetricsStore holds the latest and historical metrics in memory
type MetricsStore struct {
	mu      sync.RWMutex       // Protects concurrent access
	latest  *DeviceMetrics      // Most recent metric
	history []DeviceMetrics     // Historical metrics
	maxSize int                 // Maximum history size
}

// NewMetricsStore creates a new metrics store
func NewMetricsStore(maxSize int) *MetricsStore {
	return &MetricsStore{
		history: make([]DeviceMetrics, 0, maxSize),
		maxSize: maxSize,
	}
}

// AddMetric adds a new metric to the store
func (ms *MetricsStore) AddMetric(metric DeviceMetrics) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Update latest
	ms.latest = &metric
	
	// Add to history
	ms.history = append(ms.history, metric)

	// Keep only the last maxSize metrics
	if len(ms.history) > ms.maxSize {
		ms.history = ms.history[len(ms.history)-ms.maxSize:]
	}
}

// GetLatest returns the most recent metric (thread-safe)
func (ms *MetricsStore) GetLatest() *DeviceMetrics {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.latest
}

// GetHistory returns all historical metrics (thread-safe)
func (ms *MetricsStore) GetHistory() []DeviceMetrics {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	historyCopy := make([]DeviceMetrics, len(ms.history))
	copy(historyCopy, ms.history)
	return historyCopy
}

// ====================
// PART 2: KAFKA CONSUMER
// ====================

// KafkaConsumer consumes messages from Kafka and stores them
type KafkaConsumer struct {
	reader *kafka.Reader
	store  *MetricsStore
}

// NewKafkaConsumer creates a new Kafka consumer
func NewKafkaConsumer(brokers []string, topic string, store *MetricsStore) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        "iot-api-consumer",
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset, // Start from the most recent messages
	})

	return &KafkaConsumer{
		reader: reader,
		store:  store,
	}
}

// Start begins consuming messages from Kafka
func (kc *KafkaConsumer) Start(ctx context.Context) {
	log.Println("🔄 Starting Kafka consumer...")

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("⏹️  Stopping Kafka consumer...")
				kc.reader.Close()
				return
			default:
				// Read message from Kafka
				msg, err := kc.reader.ReadMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("❌ Error reading message: %v", err)
					continue
				}

				// Parse JSON message
				var metric DeviceMetrics
				if err := json.Unmarshal(msg.Value, &metric); err != nil {
					log.Printf("❌ Error unmarshaling message: %v", err)
					continue
				}

				// Store the metric
				kc.store.AddMetric(metric)
				log.Printf("✅ Received metric: Temp=%.1f°C, Humidity=%.1f%%, Devices=%d",
					metric.AvgTemperature, metric.AvgHumidity, metric.DeviceCount)
			}
		}
	}()
}

// ====================
// PART 3: API HANDLERS
// ====================

// APIServer handles HTTP requests
type APIServer struct {
	store *MetricsStore
}

// NewAPIServer creates a new API server
func NewAPIServer(store *MetricsStore) *APIServer {
	return &APIServer{store: store}
}

// GetLatestMetrics returns the most recent aggregated metrics
func (api *APIServer) GetLatestMetrics(w http.ResponseWriter, r *http.Request) {
	latest := api.store.GetLatest()
	
	if latest == nil {
		http.Error(w, `{"error": "No metrics available yet"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(latest)
}

// GetHistoricalMetrics returns all historical metrics
func (api *APIServer) GetHistoricalMetrics(w http.ResponseWriter, r *http.Request) {
	history := api.store.GetHistory()
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(history)
}

// HealthCheck returns the health status of the API
func (api *APIServer) HealthCheck(w http.ResponseWriter, r *http.Request) {
	latest := api.store.GetLatest()
	
	status := map[string]interface{}{
		"status":      "healthy",
		"timestamp":   time.Now().Format(time.RFC3339),
		"hasData":     latest != nil,
		"historySize": len(api.store.GetHistory()),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// ====================
// PART 4: MAIN FUNCTION
// ====================

func main() {
	log.Println("🚀 Starting IoT Analytics Backend API...")

	// Configuration
	kafkaBrokers := []string{"localhost:9092"}
	kafkaTopic := "aggregated-metrics"
	apiPort := getEnv("API_PORT", "8080")

	// Initialize metrics store (keep last 100 metrics)
	store := NewMetricsStore(100)

	// Start Kafka consumer
	consumer := NewKafkaConsumer(kafkaBrokers, kafkaTopic, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	consumer.Start(ctx)

	// Setup API server
	apiServer := NewAPIServer(store)
	router := mux.NewRouter()

	// Define API routes
	router.HandleFunc("/api/metrics/latest", apiServer.GetLatestMetrics).Methods("GET")
	router.HandleFunc("/api/metrics/history", apiServer.GetHistoricalMetrics).Methods("GET")
	router.HandleFunc("/health", apiServer.HealthCheck).Methods("GET")

	// CORS configuration (allow frontend to access API)
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000", "http://localhost:3001"},
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type"},
		AllowCredentials: true,
	})

	handler := c.Handler(router)

	// HTTP server configuration
	srv := &http.Server{
		Addr:         ":" + apiPort,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("🌐 API server listening on port %s...", apiPort)
		log.Printf("📊 Endpoints:")
		log.Printf("   - GET http://localhost:%s/health", apiPort)
		log.Printf("   - GET http://localhost:%s/api/metrics/latest", apiPort)
		log.Printf("   - GET http://localhost:%s/api/metrics/history", apiPort)
		
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Server error: %v", err)
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("⏹️  Shutting down server...")
	cancel() // Stop Kafka consumer

	// Graceful shutdown with timeout
	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	if err := srv.Shutdown(ctxShutdown); err != nil {
		log.Printf("❌ Server forced to shutdown: %v", err)
	}

	log.Println("✅ Server exited gracefully")
}

// ====================
// HELPER FUNCTIONS
// ====================

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
