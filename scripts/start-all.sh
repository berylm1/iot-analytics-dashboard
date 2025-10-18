#!/bin/bash

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Starting all services...${NC}"
echo ""

# Check if tmux is available
if command -v tmux &> /dev/null; then
    echo "Starting services in tmux session..."
    tmux new-session -d -s iot-dashboard
    tmux split-window -h
    tmux split-window -v
    tmux select-pane -t 0
    tmux split-window -v
    
    tmux send-keys -t 0 'go run cmd/producer/main.go' C-m
    tmux send-keys -t 1 'go run cmd/aggregator/main.go' C-m
    tmux send-keys -t 2 'go run cmd/api/main.go' C-m
    tmux send-keys -t 3 'cd frontend && npm start' C-m
    
    echo -e "${GREEN}✅ All services started in tmux!${NC}"
    echo "Attach with: tmux attach -t iot-dashboard"
else
    echo "tmux not found. Starting services in background..."
    go run cmd/producer/main.go > logs/producer.log 2>&1 &
    go run cmd/aggregator/main.go > logs/aggregator.log 2>&1 &
    go run cmd/api/main.go > logs/api.log 2>&1 &
    cd frontend && npm start &
    
    echo -e "${GREEN}✅ All services started in background!${NC}"
    echo "Check logs in: logs/"
fi
