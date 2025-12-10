#!/bin/bash

#############################################################################
# Developer Environment Setup Script
# Sets up development environment for Derivative Analysis
#############################################################################

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Derivative Analysis - Dev Setup${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Check prerequisites
echo -e "${YELLOW}[1/9] Checking prerequisites...${NC}"

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is not installed${NC}"
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Python ${PYTHON_VERSION} found"

if ! command -v git &> /dev/null; then
    echo -e "${RED}Error: Git is not installed${NC}"
    exit 1
fi
echo "✓ Git found"

if ! command -v psql &> /dev/null; then
    echo -e "${YELLOW}Warning: PostgreSQL client not found${NC}"
    echo "  Install: apt-get install postgresql-client (Ubuntu/Debian)"
else
    echo "✓ PostgreSQL client found"
fi

# Create/recreate virtual environment
echo ""
echo -e "${YELLOW}[2/9] Setting up virtual environment...${NC}"

if [ -d "venv" ]; then
    echo "Removing existing venv..."
    rm -rf venv
fi

python3 -m venv venv
source venv/bin/activate || . venv/Scripts/activate 2>/dev/null

echo "✓ Virtual environment created"

# Upgrade pip
echo ""
echo -e "${YELLOW}[3/9] Upgrading pip...${NC}"
pip install --upgrade pip
echo "✓ Pip upgraded"

# Install requirements
echo ""
echo -e "${YELLOW}[4/9] Installing dependencies...${NC}"
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    echo "✓ Dependencies installed"
else
    echo -e "${RED}Error: requirements.txt not found${NC}"
    exit 1
fi

# Install development tools
echo ""
echo -e "${YELLOW}[5/9] Installing development tools...${NC}"
pip install black isort flake8 pylint pytest pytest-flask pytest-cov bandit safety
echo "✓ Development tools installed"

# Setup pre-commit hooks
echo ""
echo -e "${YELLOW}[6/9] Setting up pre-commit hooks...${NC}"
if [ -f ".pre-commit-config.yaml" ]; then
    pip install pre-commit
    pre-commit install
    echo "✓ Pre-commit hooks installed"
else
    echo "⚠ .pre-commit-config.yaml not found, skipping"
fi

# Create .env from template
echo ""
echo -e "${YELLOW}[7/9] Setting up environment variables...${NC}"
if [ ! -f ".env" ]; then
    if [ -f ".env.template" ]; then
        cp .env.template .env
        echo "✓ Created .env from template"
        echo -e "${YELLOW}  ⚠ Please edit .env with your actual configuration${NC}"
    else
        echo "⚠ .env.template not found"
    fi
else
    echo "✓ .env already exists"
fi

# Create necessary directories
echo ""
echo -e "${YELLOW}[8/9] Creating directories...${NC}"
mkdir -p logs
mkdir -p Database/Backups
mkdir -p uploads
echo "✓ Directories created"

# Git configuration
echo ""
echo -e "${YELLOW}[9/9] Git configuration...${NC}"
git config --local core.autocrlf input
git config --local pull.rebase false
echo "✓ Git configured"

# Summary
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Next steps:"
echo "1. Activate virtual environment:"
echo "   source venv/bin/activate  (Linux/Mac)"
echo "   venv\\Scripts\\activate  (Windows)"
echo ""
echo "2. Edit .env with your database credentials"
echo ""
echo "3. Run the application:"
echo "   python run.py"
echo ""
echo "4. Create a feature branch:"
echo "   git checkout -b feature/my-feature"
echo ""
echo "5. Visit: http://localhost:5000"
echo ""
echo -e "${GREEN}Happy coding! 🚀${NC}"
