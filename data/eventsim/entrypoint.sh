#!/bin/bash
set -e

# 로그 디렉토리 생성
mkdir -p /var/log/eventsim

# eventsim-run.sh 실행 권한 확인
chmod +x /app/eventsim-run.sh

echo "Starting eventsim in continuous mode..."

# continuous 모드에서는 직접 실행하여 실시간 출력
exec /app/eventsim-run.sh