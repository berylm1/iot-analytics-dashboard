.PHONY: help install kafka-up kafka-down producer aggregator api frontend all clean test

help:
	@echo "IoT Analytics Dashboard - Available Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install      - Install all dependencies"
	@echo "  make kafka-up     - Start Kafka and Zookeeper"
	@echo "  make kafka-down   - Stop Kafka and Zookeeper"
	@echo ""
	@echo "Run Services:"
	@echo "  make producer     - Run IoT device producer"
	@echo "  make aggregator   - Run data aggregator"
	@echo "  make api          - Run API server"
	@echo "  make frontend     - Run React frontend"
	@echo "  make all          - Run all backend services"
	@echo ""
	@echo "Utilities:"
	@echo "  make status       - Check service status"
	@echo "  make logs         - View Kafka logs"
	@echo "  make test         - Run tests"
	@echo "  make clean        - Clean build artifacts"

install:
	@echo "Installing Go dependencies..."
	go mod download
	go mod tidy
	@echo "Installing frontend dependencies..."
	cd frontend && npm install
	@echo "✅ Installation complete!"

kafka-up:
	@echo "Starting Kafka infrastructure..."
	docker-compose up -d
	@echo "Waiting for Kafka to be ready..."
	@sleep 15
	@echo "✅ Kafka is ready!"

kafka-down:
	@echo "Stopping Kafka infrastructure..."
	docker-compose down
	@echo "✅ Kafka stopped"

producer:
	@echo "Starting IoT Device Producer..."
	go run cmd/producer/main.go

aggregator:
	@echo "Starting Data Aggregator..."
	go run cmd/aggregator/main.go

api:
	@echo "Starting API Server..."
	go run cmd/api/main.go

frontend:
	@echo "Starting React Frontend..."
	cd frontend && npm start

all:
	@echo "Starting all backend services..."
	@echo "Press Ctrl+C to stop all services"
	@trap 'kill 0' INT; \
	go run cmd/producer/main.go & \
	go run cmd/aggregator/main.go & \
	go run cmd/api/main.go & \
	wait

status:
	@./scripts/check-status.sh

logs:
	@echo "Viewing Kafka logs (Ctrl+C to exit)..."
	docker-compose logs -f kafka

test:
	@echo "Running tests..."
	go test -v ./...

clean:
	@echo "Cleaning build artifacts..."
	go clean
	rm -rf bin/
	cd frontend && rm -rf build/ node_modules/.cache
	@echo "✅ Clean complete"

build:
	@echo "Building binaries..."
	mkdir -p bin
	go build -o bin/producer cmd/producer/main.go
	go build -o bin/aggregator cmd/aggregator/main.go
	go build -o bin/api cmd/api/main.go
	@echo "✅ Build complete! Binaries in bin/"
