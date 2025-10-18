package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/berylm1/iot-analytics-dashboard/internal/api"
	"github.com/berylm1/iot-analytics-dashboard/internal/storage"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

const (
	serverPort      = ":8080"
	kafkaBroker     = "localhost:9092"
	aggregatedTopic = "iot-telemetry-aggregated"
)

func main() {
	log.Println("Starting API Server...")

	// Initialize storage
	store := storage.NewInMemoryStore()

	// Start Kafka consumer to populate storage
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := storage.NewKafkaConsumer(kafkaBroker, aggregatedTopic, store)
	go consumer.Start(ctx)

	// Setup HTTP handlers
	handler := api.NewHandler(store)
	router := setupRouter(handler)

	// Configure CORS
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000", "http://localhost:5173"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
	})

	server := &http.Server{
		Addr:         serverPort,
		Handler:      c.Handler(router),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("API Server listening on %s\n", serverPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v\n", err)
		}
	}()

	<-sigChan
	log.Println("Shutting down API server...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v\n", err)
	}

	log.Println("API Server stopped")
}

func setupRouter(handler *api.Handler) *mux.Router {
	router := mux.NewRouter()

	// Health check
	router.HandleFunc("/health", healthCheck).Methods("GET")

	// API routes
	apiRouter := router.PathPrefix("/api").Subrouter()
	apiRouter.HandleFunc("/aggregations", handler.GetAggregations).Methods("GET")
	apiRouter.HandleFunc("/aggregations/latest", handler.GetLatestAggregation).Methods("GET")
	apiRouter.HandleFunc("/stats", handler.GetStats).Methods("GET")

	return router
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	})
}
