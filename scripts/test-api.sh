#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

API_URL="http://localhost:8080"

echo "========================================"
echo "IoT Analytics Dashboard - API Tests"
echo "========================================"
echo ""

# Test 1: Health Check
echo -e "${YELLOW}Test 1: Health Check${NC}"
response=$(curl -s -o /dev/null -w "%{http_code}" $API_URL/health)
if [ $response -eq 200 ]; then
    echo -e "${GREEN}✓ Health check passed${NC}"
    curl -s $API_URL/health | python3 -m json.tool 2>/dev/null || curl -s $API_URL/health
else
    echo -e "${RED}✗ Health check failed (HTTP $response)${NC}"
fi
echo ""

# Test 2: Latest Aggregation
echo -e "${YELLOW}Test 2: Get Latest Aggregation${NC}"
response=$(curl -s $API_URL/api/aggregations/latest)
if [ -n "$response" ]; then
    echo -e "${GREEN}✓ Latest aggregation retrieved${NC}"
    echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
else
    echo -e "${RED}✗ Failed to get latest aggregation${NC}"
fi
echo ""

# Test 3: Statistics
echo -e "${YELLOW}Test 3: Get Statistics${NC}"
response=$(curl -s $API_URL/api/stats)
if [ -n "$response" ]; then
    echo -e "${GREEN}✓ Statistics retrieved${NC}"
    echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
else
    echo -e "${RED}✗ Failed to get statistics${NC}"
fi

echo ""
echo "========================================"
echo -e "${GREEN}Tests completed!${NC}"
echo "========================================"
