#!/bin/bash
set -e

cd /app

# LOG_FILE="/var/log/eventsim/eventsim.log"
# mkdir -p $(dirname "$LOG_FILE")

echo "$(date): Starting eventsim with Whistle configuration (happy users)"

# Whistle 설정 파일 확인
CONFIG_FILE="configs/Whistle-config.json"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Warning: $CONFIG_FILE not found, using default config"
    CONFIG_FILE="examples/example-config.json"
fi

echo "Using config file: $CONFIG_FILE"

# eventsim 실행 - continuous 모드
./bin/eventsim -c "examples/example-config.json" --nusers 10 --randomseed 1     --start-time "$(date -Iseconds | cut -d'+' -f1)" --end-time "$(date -Iseconds -d '+1 hour' | cut -d'+' -f1)" --continuous 