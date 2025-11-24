#!/bin/bash

# GenAI Network Monitoring System - Start with Separate Log Windows
# This script starts backend and frontend in separate terminal windows

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BACKEND_PORT=8001
FRONTEND_PORT=5173
BACKEND_DIR="./backend"
FRONTEND_DIR="./Frontend"

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  GenAI Network Monitoring System - Start with Logs          ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Function to check if directory exists
check_directory() {
    if [ ! -d "$1" ]; then
        echo -e "${RED}✗ Error: Directory $1 not found${NC}"
        echo "Please run this script from the project root directory"
        exit 1
    fi
}

# Check directories
echo -e "${YELLOW}Checking directories...${NC}"
check_directory "$BACKEND_DIR"
check_directory "$FRONTEND_DIR"
echo -e "${GREEN}✓ Directories found${NC}"
echo ""

# Check if Python is available
echo -e "${YELLOW}Checking Python...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗ Python3 not found. Please install Python 3.9+${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python found${NC}"

# Check if Node.js is available
echo -e "${YELLOW}Checking Node.js...${NC}"
if ! command -v node &> /dev/null; then
    echo -e "${RED}✗ Node.js not found. Please install Node.js 16+${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Node.js found${NC}"
echo ""

# Install dependencies if needed
echo -e "${YELLOW}Checking dependencies...${NC}"

# Backend dependencies
cd "$BACKEND_DIR"
if ! python3 -c "import fastapi" 2>/dev/null; then
    echo -e "${YELLOW}Installing Python dependencies...${NC}"
    pip install -r requirements.txt > /dev/null 2>&1
fi

# Frontend dependencies
cd "../$FRONTEND_DIR"
if [ ! -d "node_modules" ]; then
    echo -e "${YELLOW}Installing Node dependencies...${NC}"
    npm install > /dev/null 2>&1
fi

cd ..
echo -e "${GREEN}✓ Dependencies ready${NC}"
echo ""

# Function to open terminal window (works on most systems)
open_terminal() {
    local title="$1"
    local command="$2"
    local dir="$3"
    
    # Try different terminal emulators
    if command -v gnome-terminal &> /dev/null; then
        gnome-terminal --title="$title" --working-directory="$dir" -- bash -c "$command; exec bash"
    elif command -v konsole &> /dev/null; then
        konsole --title "$title" --workdir "$dir" -e bash -c "$command; exec bash"
    elif command -v xterm &> /dev/null; then
        xterm -title "$title" -e "cd '$dir' && $command; exec bash" &
    elif command -v terminal &> /dev/null; then
        terminal --title="$title" --working-directory="$dir" -- bash -c "$command; exec bash"
    else
        echo -e "${YELLOW}Could not open separate terminal. Running in background...${NC}"
        cd "$dir" && eval "$command" &
        return
    fi
}

# Start Backend in separate window
echo -e "${YELLOW}Starting Backend in separate window...${NC}"
open_terminal "Backend - FastAPI" "python3 -m uvicorn app.main:app --reload --port $BACKEND_PORT" "$BACKEND_DIR"
echo -e "${GREEN}✓ Backend started${NC}"
echo "  URL: http://localhost:$BACKEND_PORT"
echo "  API Docs: http://localhost:$BACKEND_PORT/docs"
echo ""

# Wait a moment for backend to start
sleep 3

# Start Frontend in separate window
echo -e "${YELLOW}Starting Frontend in separate window...${NC}"
open_terminal "Frontend - React" "npm run dev" "$FRONTEND_DIR"
echo -e "${GREEN}✓ Frontend started${NC}"
echo "  URL: http://localhost:$FRONTEND_PORT"
echo ""

# Wait for services to be ready
echo -e "${YELLOW}Waiting for services to be ready...${NC}"
for i in {1..30}; do
    if curl -s http://localhost:$BACKEND_PORT/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Backend is ready${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${YELLOW}⚠ Backend may still be starting...${NC}"
        break
    fi
    sleep 1
done

for i in {1..30}; do
    if curl -s http://localhost:$FRONTEND_PORT > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Frontend is ready${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${YELLOW}⚠ Frontend may still be starting...${NC}"
        break
    fi
    sleep 1
done

echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                    SERVICES RUNNING                        ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}✓ Backend${NC}  : http://localhost:$BACKEND_PORT"
echo -e "${GREEN}✓ Frontend${NC} : http://localhost:$FRONTEND_PORT"
echo ""
echo -e "${YELLOW}API Documentation${NC}: http://localhost:$BACKEND_PORT/docs"
echo ""
echo -e "${BLUE}Logs are displayed in separate terminal windows${NC}"
echo -e "${YELLOW}Close the terminal windows to stop services${NC}"
echo ""
echo -e "${YELLOW}Or press Ctrl+C here to show stop commands${NC}"

# Wait for user input
read -p "Press Enter to see stop commands or Ctrl+C to exit..."

echo ""
echo -e "${YELLOW}To stop services manually:${NC}"
echo "1. Close the terminal windows"
echo "2. Or run these commands:"
echo "   pkill -f 'uvicorn app.main:app'"
echo "   pkill -f 'npm run dev'"
echo ""

# Alternative stop commands
echo -e "${YELLOW}Alternative stop commands:${NC}"
echo "   lsof -ti:$BACKEND_PORT | xargs kill -9"
echo "   lsof -ti:$FRONTEND_PORT | xargs kill -9"
echo ""
