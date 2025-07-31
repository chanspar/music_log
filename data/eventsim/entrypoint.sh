#!/bin/bash
set -e

# 로그 디렉토리 생성
mkdir -p /var/log/eventsim

# 환경변수를 cron에서 사용할 수 있도록 설정
printenv | sed 's/^\(.*\)$/export \1/g' > /etc/profile.d/env.sh

# eventsim-run.sh 실행 권한 확인
chmod +x /app/eventsim-run.sh

# cron job 등록 - 10분마다 실행
echo "*/10 * * * * root /app/eventsim-run.sh >> /var/log/eventsim/eventsim.log 2>&1" > /etc/cron.d/eventsim-job

# cron job 권한 설정
chmod 0644 /etc/cron.d/eventsim-job

# 로그 파일 생성
touch /var/log/eventsim/eventsim.log

echo "Starting cron daemon..."
cron

echo "Eventsim scheduler started. Logs will appear every 10 minutes."
echo "You can monitor logs with: docker logs -f <container-name>"

# 로그를 실시간으로 출력
tail -f /var/log/eventsim/eventsim.log
