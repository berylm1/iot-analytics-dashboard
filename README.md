# 🌡️ IoT Analytics Dashboard

A real-time IoT analytics platform built with **Go**, **Apache Kafka**, and **React**. This system simulates IoT devices, processes telemetry data using stream processing, and visualizes metrics through an interactive dashboard.

![Dashboard Preview](https://img.shields.io/badge/Status-Production_Ready-brightgreen)
![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)
![React](https://img.shields.io/badge/React-18+-61DAFB?logo=react)
![Kafka](https://img.shields.io/badge/Kafka-3.0+-231F20?logo=apache-kafka)

## 🎯 Features

- **Real-time Data Streaming**: Apache Kafka message broker
- **Stream Processing**: 1-minute windowed aggregations
- **REST API**: Go-based API server
- **Interactive Dashboard**: React frontend with live charts
- **Scalable Architecture**: Microservices design
- **Docker Support**: Easy setup with Docker Compose

## 🏗️ Architecture
```
┌─────────────┐      ┌──────────┐      ┌────────────┐      ┌──────────┐
│  Producer   │ ───▶ │  Kafka   │ ───▶ │ Aggregator │ ───▶ │  Kafka   │
│ (5 devices) │      │  (raw)   │      │ (windows)  │      │  (agg)   │
└─────────────┘      └──────────┘      └────────────┘      └──────────┘
                                                                  │
                                                                  ▼
                                                           ┌──────────────┐
                                                           │  API Server  │
                                                           │  (REST API)  │
                                                           └──────────────┘
                                                                  │
                                                                  ▼
                                                           ┌──────────────┐
                                                           │   Frontend   │
                                                           │   (React)    │
                                                           └──────────────┘
```

## 🚀 Quick Start
## 🤖 Automated Scripts

### One-Command Setup
```bash
# Complete setup (installs dependencies, starts Kafka)
./scripts/setup.sh
```

### Using Makefile
```bash
# See all available commands
make help

# Install dependencies
make install

# Start Kafka
make kafka-up

# Run individual services
make producer      # Terminal 1
make aggregator    # Terminal 2
make api           # Terminal 3
make frontend      # Terminal 4

# Check service status
make status

# Run all backend services at once
make all

# Run tests
make test

# Stop Kafka
make kafka-down
```

### Helper Scripts
```bash
# Complete automated setup
./scripts/setup.sh

# Check all service status
./scripts/check-status.sh

# Test API endpoints
./scripts/test-api.sh

# Start all services (requires tmux)
./scripts/start-all.sh
```
### Prerequisites

- [Go 1.21+](https://golang.org/dl/)
- [Node.js 14+](https://nodejs.org/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/berylm1/iot-analytics-dashboard.git
cd iot-analytics-dashboard
```

2. **Start Kafka**
```bash
docker-compose up -d
```

3. **Install Go dependencies**
```bash
go mod download
```

4. **Install Frontend dependencies**
```bash
cd frontend
npm install
cd ..
```

### Running the Application

Open **4 separate terminals**:

**Terminal 1 - Producer:**
```bash
go run cmd/producer/main.go
```

**Terminal 2 - Aggregator:**
```bash
go run cmd/aggregator/main.go
```

**Terminal 3 - API Server:**
```bash
go run cmd/api/main.go
```

**Terminal 4 - Frontend:**
```bash
cd frontend
npm start
```

### Access the Dashboard

Open your browser to: **http://localhost:3000**

## 📊 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/api/aggregations` | GET | Get recent aggregations (query: `?limit=N`) |
| `/api/aggregations/latest` | GET | Get most recent aggregation |
| `/api/stats` | GET | Get overall statistics |

### Example API Calls
```bash
# Health check
curl http://localhost:8080/health

# Get latest data
curl http://localhost:8080/api/aggregations/latest

# Get last 10 aggregations
curl "http://localhost:8080/api/aggregations?limit=10"

# Get statistics
curl http://localhost:8080/api/stats
```

## 📁 Project Structure
```
iot-analytics-dashboard/
├── cmd/
│   ├── producer/       # IoT device simulator
│   ├── aggregator/     # Stream processor
│   └── api/            # REST API server
├── internal/
│   ├── models/         # Data models
│   ├── api/            # HTTP handlers
│   └── storage/        # In-memory storage
├── frontend/           # React dashboard
│   ├── src/
│   │   ├── App.js      # Main component
│   │   └── App.css     # Styles
│   └── package.json
├── docker-compose.yml  # Kafka infrastructure
├── go.mod
└── README.md
```

## 🔧 Configuration

Default configuration (modify in source code):

| Component | Config | Default |
|-----------|--------|---------|
| Kafka Broker | Address | localhost:9092 |
| API Server | Port | 8080 |
| Producer | Devices | 5 |
| Producer | Interval | 2 seconds |
| Aggregator | Window | 1 minute |
| Frontend | Port | 3000 |

## 🛠️ Technology Stack

### Backend
- **Go** - High-performance backend services
- **Apache Kafka** - Distributed event streaming
- **segmentio/kafka-go** - Kafka client library
- **gorilla/mux** - HTTP router
- **rs/cors** - CORS middleware

### Frontend
- **React** - UI framework
- **Recharts** - Data visualization
- **Axios** - HTTP client

### Infrastructure
- **Docker** - Containerization
- **Zookeeper** - Kafka coordination

## 📈 Data Flow

1. **Producer** generates simulated IoT telemetry (temperature, humidity)
2. **Kafka** receives and distributes raw telemetry data
3. **Aggregator** consumes raw data, computes 1-minute window statistics
4. **Kafka** receives aggregated metrics
5. **API Server** consumes aggregated data and exposes REST endpoints
6. **Frontend** fetches and visualizes data every 30 seconds

## 🧪 Testing
```bash
# Run Go tests
go test ./...

# Test API endpoints
curl http://localhost:8080/health
curl http://localhost:8080/api/stats
```

## 🐳 Docker Commands
```bash
# Start Kafka and Zookeeper
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Remove volumes
docker-compose down -v
```

## 📝 Development

### Adding New Features

1. **New API endpoint**: Add handler in `internal/api/handlers.go`
2. **New data model**: Update `internal/models/models.go`
3. **New chart**: Modify `frontend/src/App.js`

### Debugging
```bash
# Check Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# View Kafka messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic iot-telemetry-raw \
  --from-beginning
```

## 🚀 Future Enhancements

- [ ] WebSocket support for real-time updates
- [ ] PostgreSQL/MongoDB for data persistence
- [ ] User authentication and authorization
- [ ] Alert system for threshold violations
- [ ] Device management dashboard
- [ ] Historical data analysis
- [ ] Kubernetes deployment
- [ ] Grafana integration
- [ ] Multi-tenant support

## 📄 License

MIT License - feel free to use this project for learning or commercial purposes.

## 👤 Author

**Beryl Malomo**
- GitHub: [@berylm1](https://github.com/berylm1)

## 🙏 Acknowledgments

- Apache Kafka for event streaming
- React community for excellent documentation
- Go community for great libraries

---

⭐ **Star this repo if you find it helpful!**
