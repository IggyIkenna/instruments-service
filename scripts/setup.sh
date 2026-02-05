#!/usr/bin/env bash
# =============================================================================
# Instruments Service - Complete Setup Script
# =============================================================================
# This script sets up everything needed to run instruments-service:
#   - Python virtual environment
#   - All Python dependencies (including unified-cloud-services)
#   - GCP credentials detection
#
# Usage:
#   source ./scripts/setup.sh
#   ./scripts/setup.sh --help
#
# Requirements:
#   - Python 3.13 (REQUIRED for this service)
#   - SSH key configured with GitHub (for unified-cloud-services)
# =============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Determine repo root - works in bash and zsh, sourced or executed
SCRIPT_PATH=""
if [ -n "${BASH_SOURCE[0]}" ] && [ "${BASH_SOURCE[0]}" != "$0" ]; then
    SCRIPT_PATH="${BASH_SOURCE[0]}"
elif [ -n "$0" ] && [[ "$0" == *"setup.sh"* ]]; then
    SCRIPT_PATH="$0"
fi

if [ -n "$SCRIPT_PATH" ] && [ -f "$SCRIPT_PATH" ]; then
    SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
    REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
else
    # Fallback: check if we're in instruments-service or its subdirectory
    if [ -f "pyproject.toml" ]; then
        REPO_ROOT="$(pwd)"
    elif [ -f "../pyproject.toml" ]; then
        REPO_ROOT="$(cd .. && pwd)"
    elif [ -f "instruments-service/pyproject.toml" ]; then
        REPO_ROOT="$(cd instruments-service && pwd)"
    else
        # Last resort: assume current directory
        REPO_ROOT="$(pwd)"
    fi
fi

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}Instruments Service - Setup Script${NC}"
echo -e "${BLUE}============================================================${NC}"

# Parse arguments
HELP=false
for arg in "$@"; do
    case $arg in
        --help|-h)
            HELP=true
            shift
            ;;
    esac
done

if [ "$HELP" = true ]; then
    echo ""
    echo "Usage: source ./scripts/setup.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --help, -h   Show this help message"
    echo ""
    echo "This script will:"
    echo "  1. Verify Python version (MUST be 3.13.x)"
    echo "  2. Check architecture on Apple Silicon (ARM64 required)"
    echo "  3. Create/activate a virtual environment"
    echo "  4. Install unified-cloud-services from GitHub main"
    echo "  5. Install instruments-service as EDITABLE (for your changes)"
    echo "  6. Auto-detect GCP credentials"
    echo ""
    echo "TIP: Run with 'source' to auto-activate the venv when done:"
    echo "  source ./scripts/setup.sh"
    echo ""
    exit 0
fi

# =============================================================================
# PYTHON 3.13 CONFIRMATION PROMPT
# =============================================================================
echo ""
# Detect OS for platform-specific instructions
OS_TYPE="unknown"
if [[ "$OSTYPE" == "darwin"* ]]; then
    OS_TYPE="macos"
    INSTALL_CMD="brew install python@3.13"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS_TYPE="linux"
    INSTALL_CMD="sudo apt install python3.13 python3.13-venv"
else
    INSTALL_CMD="pyenv install 3.13.1 && pyenv local 3.13.1"
fi

echo -e "${YELLOW}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}║  IMPORTANT: Python 3.13.x is REQUIRED                      ║${NC}"
echo -e "${YELLOW}╠════════════════════════════════════════════════════════════╣${NC}"
echo -e "${YELLOW}║  Before continuing, ensure you have installed Python 3.13: ║${NC}"
echo -e "${YELLOW}║                                                            ║${NC}"
if [[ "$OS_TYPE" == "macos" ]]; then
echo -e "${YELLOW}║    brew install python@3.13                                ║${NC}"
echo -e "${YELLOW}║    OR: pyenv install 3.13.1 && pyenv local 3.13.1          ║${NC}"
elif [[ "$OS_TYPE" == "linux" ]]; then
echo -e "${YELLOW}║    sudo apt install python3.13 python3.13-venv             ║${NC}"
echo -e "${YELLOW}║    OR: pyenv install 3.13.1 && pyenv local 3.13.1          ║${NC}"
else
echo -e "${YELLOW}║    pyenv install 3.13.1 && pyenv local 3.13.1              ║${NC}"
fi
echo -e "${YELLOW}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -n "Have you installed Python 3.13.x? (yes/no): "
read PYTHON_CONFIRMED
if [[ ! "$PYTHON_CONFIRMED" =~ ^[Yy][Ee]?[Ss]?$ ]]; then
    echo ""
    echo -e "${YELLOW}Please install Python 3.13 first:${NC}"
    echo ""
    echo "  pyenv install 3.13.1 && pyenv local 3.13.1"
    echo "  OR: brew install python@3.13"
    echo ""
    echo "Then run this script again."
    exit 1
fi

# =============================================================================
# Step 0: Verify we're in instruments-service and pull latest
# =============================================================================
echo ""
echo -e "${YELLOW}Step 0: Checking instruments-service...${NC}"

# Verify we're inside the instruments-service repo
if [ ! -f "$REPO_ROOT/pyproject.toml" ]; then
    echo -e "${RED}ERROR: Cannot find pyproject.toml${NC}"
    echo ""
    echo "Looking in: $REPO_ROOT"
    echo "Current dir: $(pwd)"
    echo ""
    echo "Make sure you're in the instruments-service directory:"
    echo ""
    echo "  cd instruments-service"
    echo "  source ./scripts/setup.sh"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ Found instruments-service at: $REPO_ROOT${NC}"

# Pull latest (optional - don't fail if offline)
cd "$REPO_ROOT"
if [ -d ".git" ]; then
    echo "Pulling latest changes..."
    git pull origin main --quiet 2>/dev/null || git pull --quiet 2>/dev/null || echo -e "${YELLOW}⚠ Could not pull (offline or no remote)${NC}"
    echo -e "${GREEN}✓ instruments-service is ready${NC}"
fi

# =============================================================================
# Step 1: Verify Python version (MUST be 3.13.x)
# =============================================================================
echo ""
echo -e "${YELLOW}Step 1: Verifying Python version (MUST be 3.13.x)...${NC}"

PYTHON_CMD=""
# Check for python3.13 first, then python3, then python
for cmd in python3.13 python3 python; do
    if command -v $cmd &> /dev/null; then
        VERSION=$($cmd --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
        MAJOR=$(echo $VERSION | cut -d. -f1)
        MINOR=$(echo $VERSION | cut -d. -f2)

        # ONLY accept Python 3.13
        if [ "$MAJOR" -eq 3 ] && [ "$MINOR" -eq 13 ]; then
            PYTHON_CMD=$cmd
            echo -e "${GREEN}✓ Found $cmd (Python $VERSION) - REQUIRED version${NC}"
            break
        elif [ "$MAJOR" -eq 3 ] && [ "$MINOR" -eq 12 ]; then
            echo -e "${RED}✗ Found Python $VERSION - NOT SUPPORTED${NC}"
            echo -e "${RED}  This service requires Python 3.13.x${NC}"
        elif [ "$MAJOR" -eq 3 ] && [ "$MINOR" -eq 11 ]; then
            echo -e "${RED}✗ Found Python $VERSION - NOT SUPPORTED${NC}"
            echo -e "${RED}  This service requires Python 3.13.x${NC}"
        fi
    fi
done

if [ -z "$PYTHON_CMD" ]; then
    echo ""
    echo -e "${RED}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║  ERROR: Python 3.13 is REQUIRED                            ║${NC}"
    echo -e "${RED}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Install Python 3.13:"
    echo ""
    echo "  macOS (Homebrew):"
    echo "    brew install python@3.13"
    echo "    # Then add to PATH or use pyenv:"
    echo "    pyenv install 3.13.1 && pyenv local 3.13.1"
    echo ""
    echo "  Ubuntu/Debian:"
    echo "    sudo add-apt-repository ppa:deadsnakes/ppa"
    echo "    sudo apt update && sudo apt install python3.13 python3.13-venv"
    echo ""
    echo "  pyenv (recommended for version management):"
    echo "    pyenv install 3.13.1"
    echo "    pyenv local 3.13.1"
    echo ""
    exit 1
fi

# =============================================================================
# Step 1b: Architecture check (Apple Silicon)
# =============================================================================
if [[ "$OSTYPE" == "darwin"* ]]; then
    ARCH=$(uname -m)
    PYTHON_ARCH=$($PYTHON_CMD -c "import platform; print(platform.machine())")
    if [ "$ARCH" = "arm64" ] && [ "$PYTHON_ARCH" = "x86_64" ]; then
        echo ""
        echo -e "${RED}╔════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${RED}║  ERROR: Architecture mismatch detected                     ║${NC}"
        echo -e "${RED}╠════════════════════════════════════════════════════════════╣${NC}"
        echo -e "${RED}║  Your Mac: Apple Silicon (ARM64)                           ║${NC}"
        echo -e "${RED}║  Your Python: x86_64 (Intel via Rosetta)                   ║${NC}"
        echo -e "${RED}║                                                            ║${NC}"
        echo -e "${RED}║  This will cause package installation failures             ║${NC}"
        echo -e "${RED}║  (pydantic_core, numpy, etc.)                              ║${NC}"
        echo -e "${RED}╚════════════════════════════════════════════════════════════╝${NC}"
        echo ""
        echo "Fix: Install native ARM64 Python 3.13:"
        echo "  brew install python@3.13"
        echo "  OR: pyenv install 3.13.1 && pyenv local 3.13.1"
        echo ""
        exit 1
    fi
fi

# =============================================================================
# Step 2: Create virtual environment
# =============================================================================
echo ""
echo -e "${YELLOW}Step 2: Setting up virtual environment...${NC}"

VENV_DIR="$REPO_ROOT/.venv"

if [ -d "$VENV_DIR" ]; then
    echo -e "${GREEN}✓ Virtual environment already exists at $VENV_DIR${NC}"
else
    echo "Creating virtual environment..."
    $PYTHON_CMD -m venv "$VENV_DIR"
    echo -e "${GREEN}✓ Created virtual environment at $VENV_DIR${NC}"
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"
echo -e "${GREEN}✓ Activated virtual environment${NC}"

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip > /dev/null 2>&1
echo -e "${GREEN}✓ pip upgraded${NC}"

# =============================================================================
# Step 3: Install unified-cloud-services (always from GitHub main)
# =============================================================================
echo ""
echo -e "${YELLOW}Step 3: Installing unified-cloud-services from GitHub...${NC}"

# Ensure SSH agent has keys loaded (pip spawns subprocess that needs this)
if [ -z "$SSH_AUTH_SOCK" ]; then
    eval "$(ssh-agent -s)" > /dev/null 2>&1
fi
# Add default SSH keys to agent (silently, in case they're already added)
ssh-add ~/.ssh/id_ed25519 2>/dev/null || ssh-add ~/.ssh/id_rsa 2>/dev/null || true

UCS_SSH_URL="git+ssh://git@github.com/IggyIkenna/unified-cloud-services.git"

echo "Installing unified-cloud-services..."

# Install using modern pip syntax: 'package @ URL'
if ! pip install "unified-cloud-services @ ${UCS_SSH_URL}"; then
    echo ""
    echo -e "${RED}✗ Failed to install unified-cloud-services${NC}"
    echo ""
    echo "This is likely an SSH authentication issue. Options:"
    echo ""
    echo "  1. Set up SSH key with GitHub:"
    echo "     ssh-keygen -t ed25519 -C 'your_email@example.com'"
    echo "     cat ~/.ssh/id_ed25519.pub"
    echo "     # Add to: https://github.com/settings/keys"
    echo ""
    echo "  2. Or install manually with a Personal Access Token (PAT):"
    echo "     pip install 'unified-cloud-services @ git+https://YOUR_PAT@github.com/IggyIkenna/unified-cloud-services.git'"
    echo ""
    echo "  3. Or clone manually and install:"
    echo "     git clone git@github.com:IggyIkenna/unified-cloud-services.git ../unified-cloud-services"
    echo "     pip install -e ../unified-cloud-services"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ Installed unified-cloud-services${NC}"

# =============================================================================
# Step 4: Install instruments-service
# =============================================================================
echo ""
echo -e "${YELLOW}Step 4: Installing instruments-service...${NC}"

cd "$REPO_ROOT"

# Install with dev dependencies
echo "Installing instruments-service with all dependencies..."
pip install -e ".[dev]" > /dev/null 2>&1
echo -e "${GREEN}✓ Installed instruments-service${NC}"

# Verify installation
if python -c "import instruments_service; print('OK')" 2>/dev/null | grep -q "OK"; then
    echo -e "${GREEN}✓ instruments_service module importable${NC}"
else
    echo -e "${RED}✗ Failed to import instruments_service${NC}"
    exit 1
fi

# =============================================================================
# Step 5: Check GCP credentials
# =============================================================================
echo ""
echo -e "${YELLOW}Step 5: Checking GCP credentials...${NC}"

# Look for credentials file in repo root
CREDS_FILE=$(find "$REPO_ROOT" -maxdepth 1 -name "central-element-*.json" -type f 2>/dev/null | head -1)

if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ] && [ -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo -e "${GREEN}✓ GCP credentials found at $GOOGLE_APPLICATION_CREDENTIALS${NC}"
elif [ -n "$CREDS_FILE" ]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$CREDS_FILE"
    echo -e "${GREEN}✓ GCP credentials found: $(basename $CREDS_FILE)${NC}"
    echo -e "${GREEN}✓ Set GOOGLE_APPLICATION_CREDENTIALS=$CREDS_FILE${NC}"
else
    echo -e "${YELLOW}⚠ GCP credentials not found${NC}"
    echo "  Place your service account JSON file in: $REPO_ROOT"
    echo "  Or set: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json"
fi

# =============================================================================
# Summary and Auto-Activation
# =============================================================================
echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""

# Check if script was sourced (activation will persist) or executed (won't persist)
if [[ "${BASH_SOURCE[0]}" != "${0}" ]] 2>/dev/null || [[ "$ZSH_EVAL_CONTEXT" == *:file:* ]] 2>/dev/null; then
    # Script was sourced - venv activation persists!
    echo -e "${GREEN}✓ Virtual environment is now ACTIVE${NC}"
    echo ""
    echo "You can now run:"
    echo -e "  ${BLUE}python -m instruments_service --help${NC}"
    echo ""
    echo "Known working command (May 23, 2023):"
    echo -e "  ${BLUE}python -m instruments_service --category CEFI --start-date 2023-05-23 --end-date 2023-05-23 --dry-run${NC}"
else
    # Script was executed - need to activate manually
    echo -e "${YELLOW}Run this command to activate the virtual environment:${NC}"
    echo ""
    echo -e "  ${BLUE}source .venv/bin/activate${NC}"
    echo ""
    echo -e "${YELLOW}TIP: Next time, run with 'source' for auto-activation:${NC}"
    echo -e "  ${BLUE}source ./scripts/setup.sh${NC}"
    echo ""
    echo "Then run:"
    echo -e "  ${BLUE}python -m instruments_service --help${NC}"
fi
echo ""
echo "Run quality gates:"
echo -e "  ${BLUE}./scripts/quality-gates.sh${NC}"
echo ""
