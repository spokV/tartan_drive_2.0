#!/bin/bash

LOG_DIR="logs"
LOG_FILE="$LOG_DIR/download_$(date +%Y%m%d_%H%M%S).log"

usage() {
    echo "Usage: $0 [-h] <kitti|bags> <save_path>"
    echo "Download dataset from MinIO storage"
    echo ""
    echo "Options:"
    echo "  -h    Show this help message and exit"
    echo ""
    echo "Arguments:"
    echo "  <kitti|bags>   Type of data to download (required)"
    echo "  <save_path>    Directory to save downloaded files (required)"
    echo ""
    echo "Example:"
    echo "  $0 kitti ./data"
}

# Handle help option
if [ "$1" = "-h" ]; then
    usage
    exit 0
fi

# Validate arguments
if [ $# -ne 2 ]; then
    echo "Error: Invalid number of arguments"
    usage
    exit 1
fi

TYPE=$1
SAVE_PATH=$2

# Create directories if they don't exist
mkdir -p "$SAVE_PATH"
mkdir -p "$LOG_DIR"

# Run download process in background with output logging
setsid python3 scripts/download_dataset.py --type "$TYPE" --save-path "$SAVE_PATH" > "$LOG_FILE" 2>&1 &

# Disown process to detach from terminal
disown

echo "Dataset download started in background. Tailing log output:"
echo "Log file: $LOG_FILE"
tail -f "$LOG_FILE"