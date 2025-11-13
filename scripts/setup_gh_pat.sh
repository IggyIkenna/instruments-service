#!/bin/bash
# Helper script to set up GH_PAT in .env file

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"

echo -e "${BLUE}🔐 GitHub Personal Access Token Setup${NC}"
echo ""

# Check if .env file exists
if [ ! -f "$ENV_FILE" ]; then
    echo -e "${RED}❌ .env file not found at $ENV_FILE${NC}"
    exit 1
fi

# Check if GH_PAT is already set
if grep -q "^GH_PAT=" "$ENV_FILE" && ! grep -q "^GH_PAT=$" "$ENV_FILE"; then
    echo -e "${YELLOW}⚠️  GH_PAT is already set in .env file${NC}"
    read -p "Do you want to update it? (y/N): " UPDATE
    if [[ ! "$UPDATE" =~ ^[Yy]$ ]]; then
        echo -e "${GREEN}✅ Keeping existing GH_PAT${NC}"
        exit 0
    fi
fi

echo -e "${BLUE}📝 Follow these steps to create a GitHub Personal Access Token:${NC}"
echo ""
echo "1. Open your browser and go to:"
echo "   ${BLUE}https://github.com/settings/tokens/new${NC}"
echo ""
echo "2. Fill in the form:"
echo "   - Note: ${GREEN}instruments-service-unified-cloud-services${NC}"
echo "   - Expiration: Choose your preferred expiration (90 days recommended)"
echo "   - Scopes: Check the following:"
echo "     ✅ ${GREEN}repo${NC} (Full control of private repositories)"
echo "     ✅ ${GREEN}read:packages${NC} (Download packages from GitHub Package Registry)"
echo ""
echo "3. Click 'Generate token'"
echo "4. Copy the token (you'll only see it once!)"
echo ""
read -sp "Paste your GitHub Personal Access Token here: " PAT
echo ""

if [ -z "$PAT" ]; then
    echo -e "${YELLOW}⚠️  No token provided. Exiting.${NC}"
    exit 1
fi

# Update or add GH_PAT in .env file
if grep -q "^GH_PAT=" "$ENV_FILE"; then
    # Replace existing GH_PAT
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sed -i '' "s|^GH_PAT=.*|GH_PAT=$PAT|" "$ENV_FILE"
    else
        # Linux
        sed -i "s|^GH_PAT=.*|GH_PAT=$PAT|" "$ENV_FILE"
    fi
    echo -e "${GREEN}✅ Updated GH_PAT in .env file${NC}"
else
    # Add GH_PAT if it doesn't exist
    echo "" >> "$ENV_FILE"
    echo "GH_PAT=$PAT" >> "$ENV_FILE"
    echo -e "${GREEN}✅ Added GH_PAT to .env file${NC}"
fi

echo ""
echo -e "${GREEN}✅ Setup complete!${NC}"
echo ""
echo "You can now run:"
echo "  python scripts/run_quality_gates.py"
echo ""
echo "The script will automatically use GH_PAT from your .env file."

