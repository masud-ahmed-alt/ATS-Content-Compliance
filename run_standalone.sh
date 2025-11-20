#!/bin/bash
# ============================================================================
# CCompliance System - Standalone Run Script (Linux/Mac)
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=========================================="
echo "CCompliance System - Standalone Mode"
echo "==========================================${NC}"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${RED}Error: Virtual environment not found.${NC}"
    echo "Please run: ./setup_venv.sh"
    exit 1
fi

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source venv/bin/activate

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}Warning: .env file not found.${NC}"
    if [ -f ".env.example" ]; then
        echo "Copying .env.example to .env..."
        cp .env.example .env
        echo -e "${YELLOW}Please edit .env file with your configuration.${NC}"
    fi
fi

# Load environment variables
if [ -f ".env" ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

echo -e "${GREEN}Starting services...${NC}"
echo ""
echo "Available services:"
echo "1. Python Analyzer (port 8000)"
echo "2. Playwright Renderer (port 9000)"
echo "3. Go Fetcher (port 8080)"
echo "4. Frontend (port 5173)"
echo ""
echo "Press Ctrl+C to stop all services"
echo ""

# Start Python Analyzer in background
echo -e "${YELLOW}Starting Python Analyzer...${NC}"
cd python-analyzer
python app.py &
PYTHON_PID=$!
cd ..

# Wait a bit for services to start
sleep 2

echo -e "${GREEN}Python Analyzer started (PID: $PYTHON_PID)${NC}"
echo ""
echo "Services are running. Check logs above for any errors."
echo ""
echo "To stop services, press Ctrl+C or run:"
echo "  kill $PYTHON_PID"

# Wait for user interrupt
trap "echo ''; echo 'Stopping services...'; kill $PYTHON_PID 2>/dev/null; exit" INT TERM

# Keep script running
wait

