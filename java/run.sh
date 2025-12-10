#!/bin/bash
# One Billion Row Challenge - Java Runner Script (Unix/Linux/Mac)

# Color codes
BLUE='\033[1;34m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
RESET='\033[0m'

echo ""
echo -e "${CYAN}===================================${RESET}"
echo -e "${CYAN} One Billion Row Challenge - Java${RESET}"
echo -e "${CYAN}===================================${RESET}"
echo ""

# Check if classes directory exists
if [ ! -d "target/classes" ]; then
    echo -e "${YELLOW}Building project first...${RESET}"
    make build
    if [ $? -ne 0 ]; then
        echo -e "${RED}Build failed!${RESET}"
        exit 1
    fi
fi

# Run the application
echo -e "${BLUE}Running benchmark...${RESET}"
echo ""
cd target/classes
java com.onebillion.Main "$@"
cd ../..

echo ""
echo -e "${GREEN}Done!${RESET}"
