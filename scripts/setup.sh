#!/bin/bash

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}"
cat << "BANNER"
╔════════════════════════════════════════╗
║   IoT Analytics Dashboard - Setup     ║
╚════════════════════════════════════════╝
BANNER
echo -e "${NC}"

echo -e "${YELLOW}Step 1/3: Installing Go dependencies...${NC}"
go mod download
go mod tidy

echo ""
echo -e "${YELLOW}Step 2/3: Installing frontend dependencies...${NC}"
cd frontend && npm install && cd ..

echo ""
echo -e "${YELLOW}Step 3/3: Starting Kafka infrastructure...${NC}"
docker-compose up -d
sleep 15

echo ""
echo -e "${GREEN}✅ Setup complete!${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "1. Run: ${YELLOW}make producer${NC}     (in terminal 1)"
echo "2. Run: ${YELLOW}make aggregator${NC}   (in terminal 2)"
echo "3. Run: ${YELLOW}make api${NC}          (in terminal 3)"
echo "4. Run: ${YELLOW}make frontend${NC}     (in terminal 4)"
echo ""
echo "Or run everything at once: ${YELLOW}make all${NC} (backend only)"
echo ""
echo "Check status anytime: ${YELLOW}make status${NC}"
