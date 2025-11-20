#!/bin/bash
# ============================================================================
# CCompliance System - Virtual Environment Setup Script (Linux/Mac)
# ============================================================================

set -e  # Exit on error

echo "=========================================="
echo "CCompliance System - Venv Setup"
echo "=========================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check Python version
echo -e "${YELLOW}Checking Python version...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: python3 not found. Please install Python 3.8 or higher.${NC}"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
REQUIRED_VERSION="3.8"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    echo -e "${RED}Error: Python 3.8 or higher required. Found: $PYTHON_VERSION${NC}"
    exit 1
fi

echo -e "${GREEN}Python version OK: $(python3 --version)${NC}"

# Create virtual environment
echo -e "${YELLOW}Creating virtual environment...${NC}"
if [ -d "venv" ]; then
    echo -e "${YELLOW}Virtual environment already exists. Skipping creation.${NC}"
else
    python3 -m venv venv
    echo -e "${GREEN}Virtual environment created.${NC}"
fi

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source venv/bin/activate

# Upgrade pip
echo -e "${YELLOW}Upgrading pip...${NC}"
pip install --upgrade pip setuptools wheel

# Install Python analyzer dependencies
echo -e "${YELLOW}Installing Python analyzer dependencies...${NC}"
if [ -f "python-analyzer/requirements.txt" ]; then
    pip install -r python-analyzer/requirements.txt
    echo -e "${GREEN}Python analyzer dependencies installed.${NC}"
else
    echo -e "${RED}Warning: python-analyzer/requirements.txt not found.${NC}"
fi

# Install Playwright renderer dependencies
echo -e "${YELLOW}Installing Playwright renderer dependencies...${NC}"
if [ -f "playwright-renderer/requirements.txt" ]; then
    pip install -r playwright-renderer/requirements.txt
    echo -e "${GREEN}Playwright renderer dependencies installed.${NC}"
else
    echo -e "${RED}Warning: playwright-renderer/requirements.txt not found.${NC}"
fi

# Download spaCy model
echo -e "${YELLOW}Downloading spaCy model...${NC}"
python3 -m spacy download en_core_web_sm || echo -e "${YELLOW}Warning: Failed to download spaCy model. You can install it later with: python -m spacy download en_core_web_sm${NC}"

# Install Playwright browsers
echo -e "${YELLOW}Installing Playwright browsers...${NC}"
if command -v playwright &> /dev/null; then
    playwright install chromium || echo -e "${YELLOW}Warning: Failed to install Playwright browsers.${NC}"
else
    echo -e "${YELLOW}Warning: Playwright not installed. Skipping browser installation.${NC}"
fi

# Create .env file if it doesn't exist
echo -e "${YELLOW}Setting up environment file...${NC}"
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo -e "${GREEN}Created .env from .env.example${NC}"
        echo -e "${YELLOW}Please edit .env file with your configuration.${NC}"
    else
        echo -e "${YELLOW}Warning: .env.example not found. Please create .env manually.${NC}"
    fi
else
    echo -e "${GREEN}.env file already exists.${NC}"
fi

# Create data directory
echo -e "${YELLOW}Creating data directory...${NC}"
mkdir -p data
mkdir -p python-analyzer/data
echo -e "${GREEN}Data directories created.${NC}"

echo ""
echo -e "${GREEN}=========================================="
echo "Setup completed successfully!"
echo "==========================================${NC}"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your configuration"
echo "2. Start required services (PostgreSQL, Redis, MinIO, OpenSearch)"
echo "3. Activate virtual environment: source venv/bin/activate"
echo "4. Run Python analyzer: cd python-analyzer && python app.py"
echo ""
echo "To activate the virtual environment in the future:"
echo "  source venv/bin/activate"
echo ""

