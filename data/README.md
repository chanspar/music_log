###  Music Streaming Simulator (현실적 데이터 생성기)
Python 기반의 대규모 음악 스트리밍 데이터 시뮬레이터로, 실제 오디오 플랫폼(예: 스포티파이, 애플뮤직) 사용자 행동을 모방한 이벤트 스트림과 JSON 파일을 생성합니다.

####  주요 기능
**🎼 방대한 음악 데이터베이스**

- 약 10,558명 아티스트 (메가스타·톱스타·유명·인디 티어)

- 약 80만 곡 이상의 트랙: 현실적인 제목·장르·재생 시간·발매 연도·인기도·재생 횟수·좋아요 수 반영

**👥 현실적인 사용자 프로필**

- 사용자 수 자유 설정 (기본 1,000~10,000+)

- 연령대(10대~60대 이상)별 장르 선호, 활동 수준(low/medium/high), 무료·유료 등급, 위치, 가입 일자

**🔄 행동 상태 기계(State Machine)**

- 페이지: Home, NextSong, Search, Browse, Thumbs Up/Down, Add to Playlist, Settings, Logout

- 페이지별 확률적 전이

**🤖 지능형 트랙 선택 알고리즘**

- Mainstream 유저: 차트곡 60%, 선호 장르곡 30%, 랜덤 10%

- Indie 유저: 인디곡 50%, 선호 장르곡 30%, 랜덤 20%

- Mixed 유저: 균형 잡힌 선택

**📡 출력 옵션**

- Kafka 프로듀서 (브로커·토픽 지정)

- JSON 파일(라인 단위)

- 스트리밍과 파일 백업 동시 지원

**⏲ 유연한 실행**

- `--duration [분]` : 지정 시간 실행

- `--continuous` : 무제한 연속 실행

**📊 모니터링 & 샘플링**

- 최초 3개 이벤트 샘플 출력

- 500개 단위로 진행 상황 & 활성 사용자 수 로깅

**설치**
```bash
git clone <레포지토리_URL>
cd <레포지토리_디렉토리>
pip install kafka-python faker numpy
```
또는 requirements.txt 사용:
```bash
pip install -r requirements.txt
```

**사용법**
```bash
python music_streaming_producer_realistic.py \
  [--brokers 브로커1:9092,브로커2:9092] \
  [--topic Kafka_토픽] \
  [--output output.json] \
  [--users 사용자수] \
  [--duration 실행시간(분)] \
  [--continuous]
```

`--brokers` : Kafka 브로커 주소(콤마 구분)

`--topic` : Kafka 토픽 이름(사전 생성 필요)

`--output` : 출력 JSON 파일 경로

`--users` : 시뮬레이션할 사용자 수 (기본 1000)

`--duration`: 실행 시간(분)

`--continuous`: 무제한 연속 실행

**예시**
소규모 테스트 (JSON만)

```bash
python music_streaming_producer_realistic.py \
  --output test_data.json \
  --users 100 \
  --duration 5
```

<br>

프로덕션 스케일 (Kafka+JSON, 1시간)
```bash
python music_streaming_producer_realistic.py \
  --brokers localhost:9092,localhost:9093,localhost:9094 \
  --topic music-streaming-events \
  --output prod.json \
  --users 10000 \
  --duration 60
```

<br>

연속 스트리밍

```bash
python music_streaming_producer_realistic.py \
  --brokers localhost:9092 \
  --topic music-events \
  --users 5000 \
  --continuous
```

<br>

부하 테스트 (고부하)

```bash
python music_streaming_producer_realistic.py \
  --brokers localhost:9092 \
  --topic stress-test \
  --users 50000 \
  --duration 30
```

<br>

Kafka 토픽 설정
```bash
docker exec kafka1 kafka-topics.sh --create \
  --topic music-streaming-events \
  --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092 \
  --partitions 12 \
  --replication-factor 3
```

<br>

데이터 스키마
```text
{
  "ts": 1704123456789,          // 타임스탬프(ms)
  "userId": 1247,               // 사용자 ID
  "sessionId": 89234,           // 세션 ID
  "page": "NextSong",           // 페이지/행동
  "method": "PUT",              // HTTP 메소드
  "status": 200,                // HTTP 상태
  "level": "paid",              // 유료/무료 구분
  "itemInSession": 7,           // 세션 내 순번
  "location": "Seoul, KR",      // 위치
  "age": 23,                    // 나이
  "artist": "BTS",              // NextSong 시 필드
  "song": "Love Dream Forever", // NextSong 시 필드
  "length": 243,                // 곡 길이(초)
  "artist_tier": "mega",        // 아티스트 티어
  "song_popularity": 98         // 곡 인기도
}
```

<br>

**트러블슈팅**
- 토픽 없음 오류: 토픽을 먼저 생성하세요

- 메모리 부족: --users 값을 줄이거나 시스템 모니터링 후 실행

- Kafka 연결 실패: 브로커 주소·포트 확인