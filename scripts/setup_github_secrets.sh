#!/bin/bash
# Setup GitHub Secrets for Quality Gates Workflow
#
# This script helps you set up the required GitHub secrets for the quality gates workflow.
# It requires GitHub CLI (gh) to be installed and authenticated.
#
# Usage:
#   ./scripts/setup_github_secrets.sh
#
# Prerequisites:
#   1. Install GitHub CLI: brew install gh (macOS) or see https://cli.github.com/
#   2. Authenticate: gh auth login
#   3. Ensure you're in the instruments-service directory

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔐 GitHub Secrets Setup for Quality Gates${NC}"
echo ""

# Check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    echo -e "${RED}❌ GitHub CLI (gh) is not installed${NC}"
    echo ""
    echo "Install it with:"
    echo "  macOS: brew install gh"
    echo "  Linux: See https://cli.github.com/"
    echo ""
    echo "Then authenticate with: gh auth login"
    exit 1
fi

# Check if authenticated
if ! gh auth status &> /dev/null; then
    echo -e "${RED}❌ Not authenticated with GitHub CLI${NC}"
    echo ""
    echo "Run: gh auth login"
    exit 1
fi

# Get repository name (assumes we're in the repo)
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || echo "")

if [ -z "$REPO" ]; then
    echo -e "${YELLOW}⚠️  Could not detect repository. Please run this from the instruments-service directory.${NC}"
    read -p "Enter repository (format: owner/repo): " REPO
fi

echo -e "${GREEN}✅ Repository: $REPO${NC}"
echo ""

# Check if GCP credentials file exists
CREDENTIALS_FILE="central-element-323112-e35fb0ddafe2.json"
if [ ! -f "$CREDENTIALS_FILE" ]; then
    echo -e "${YELLOW}⚠️  Credentials file not found: $CREDENTIALS_FILE${NC}"
    read -p "Enter path to GCP service account JSON file: " CREDENTIALS_FILE
    if [ ! -f "$CREDENTIALS_FILE" ]; then
        echo -e "${RED}❌ File not found: $CREDENTIALS_FILE${NC}"
        exit 1
    fi
fi

# Set GCP_SERVICE_ACCOUNT_JSON secret
echo -e "${BLUE}📝 Setting GCP_SERVICE_ACCOUNT_JSON secret...${NC}"
if gh secret set GCP_SERVICE_ACCOUNT_JSON --repo "$REPO" < "$CREDENTIALS_FILE"; then
    echo -e "${GREEN}✅ GCP_SERVICE_ACCOUNT_JSON secret set successfully${NC}"
else
    echo -e "${RED}❌ Failed to set GCP_SERVICE_ACCOUNT_JSON secret${NC}"
    exit 1
fi

echo ""

# Check if GH_PAT is already set (note: GitHub doesn't allow GITHUB_ prefix)
if gh secret list --repo "$REPO" | grep -q "GH_PAT"; then
    echo -e "${YELLOW}ℹ️  GH_PAT secret already exists${NC}"
    read -p "Do you want to update it? (y/N): " UPDATE_PAT
    if [[ ! "$UPDATE_PAT" =~ ^[Yy]$ ]]; then
        echo -e "${GREEN}✅ Keeping existing GH_PAT${NC}"
        SKIP_PAT=true
    fi
fi

if [ "$SKIP_PAT" != "true" ]; then
    echo -e "${BLUE}📝 Setting GH_PAT secret...${NC}"
    echo ""
    echo "You need a GitHub Personal Access Token (PAT) with 'repo' scope."
    echo "Create one at: https://github.com/settings/tokens/new"
    echo ""
    echo "Required scopes:"
    echo "  ✅ repo (Full control of private repositories)"
    echo "  ✅ read:packages (Download packages from GitHub Package Registry)"
    echo ""
    read -sp "Enter your GitHub PAT: " PAT
    echo ""

    if [ -z "$PAT" ]; then
        echo -e "${YELLOW}⚠️  No PAT provided. Skipping GH_PAT setup.${NC}"
        echo "You can set it later manually or run this script again."
    else
        if echo -n "$PAT" | gh secret set GH_PAT --repo "$REPO"; then
            echo -e "${GREEN}✅ GH_PAT secret set successfully${NC}"
        else
            echo -e "${RED}❌ Failed to set GH_PAT secret${NC}"
            exit 1
        fi
    fi
fi

echo ""
echo -e "${GREEN}✅ Setup complete!${NC}"
echo ""
echo "Summary of secrets set:"
gh secret list --repo "$REPO" | grep -E "(GCP_SERVICE_ACCOUNT_JSON|GH_PAT)" || echo "  (No matching secrets found)"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "1. Push a commit to trigger the quality gates workflow"
echo "2. Check the Actions tab to see if it runs successfully"
echo ""
