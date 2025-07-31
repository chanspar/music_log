#!/bin/bash
set -e

cd /app

# 현재 시간과 10분 후 시간을 ISO8601 형식으로 생성
START_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
END_TIME=$(date -u -d "+10 minutes" +"%Y-%m-%dT%H:%M:%S")

echo "$(date): Starting eventsim from $START_TIME to $END_TIME"

# eventsim 실행 - JSON 형태로 출력 # 나중에 5만명 정도 늘려볼 것
./bin/eventsim -c examples/example-config.json \
    --start-time "$START_TIME" \
    --end-time "$END_TIME" \
    --nusers 3000 \
    --growth-rate 0.2 \
    --attrition-rate 0.1 \
    --randomseed $RANDOM

echo "$(date): Eventsim completed successfully"
