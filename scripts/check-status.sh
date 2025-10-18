#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================"
echo "IoT Analytics Dashboard - Status Check"
echo -e "========================================${NC}"
echo ""

# Check Docker Services
echo -e "${YELLOW}Docker Services:${NC}"
if docker ps | grep -q "kafka"; then
    echo -e "${GREEN}✓ Kafka is running${NC}"
else
    echo -e "${RED}✗ Kafka is not running${NC}"
    echo "  Run: make kafka-up"
fi

if docker ps | grep -q "zookeeper"; then
    echo -e "${GREEN}✓ Zookeeper is running${NC}"
else
    echo -e "${RED}✗ Zookeeper is not running${NC}"
fi

echo ""

# Check API Server
echo -e "${YELLOW}API Server:${NC}"
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓ API Server is running on port 8080${NC}"
else
    echo -e "${RED}✗ API Server is not running${NC}"
    echo "  Run: make api"
fi

echo ""

# Check Go Services
echo -e "${YELLOW}Go Services:${NC}"
if pgrep -f "cmd/producer/main.go" > /dev/null; then
    echo -e "${GREEN}✓ Producer is running${NC}"
else
    echo -e "${RED}✗ Producer is not running${NC}"
    echo "  Run: make producer"
fi

if pgrep -f "cmd/aggregator/main.go" > /dev/null; then
    echo -e "${GREEN}✓ Aggregator is running${NC}"
else
    echo -e "${RED}✗ Aggregator is not running${NC}"
    echo "  Run: make aggregator"
fi

echo ""

# Check Frontend
echo -e "${YELLOW}Frontend:${NC}"
if lsof -i :3000 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Frontend is running on port 3000${NC}"
else
    echo -e "${RED}✗ Frontend is not running${NC}"
    echo "  Run: make frontend"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
