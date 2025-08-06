# music_streaming_producer_realistic.py
import json
import time
import random
import threading
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import numpy as np
import argparse
import os

class RealisticMusicStreamingSimulator:
    def __init__(self, kafka_brokers=None, topic_name=None, num_users=100, output_file=None):
        self.kafka_brokers = kafka_brokers
        self.topic_name = topic_name
        self.num_users = num_users
        self.output_file = output_file
        self.fake = Faker()
        
        # Kafka Producer 설정
        self.producer = None
        if kafka_brokers and topic_name:
            self.producer = KafkaProducer(
                bootstrap_servers=kafka_brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8')
            )
        
        # JSON 파일 출력 설정
        self.json_output = None
        if output_file:
            self.json_output = open(output_file, 'w', encoding='utf-8')
        
        # 현실적인 대규모 음악 데이터베이스 구성
        print("🎼 대규모 음악 데이터베이스 생성 중... (실제 음원 서비스처럼)")
        self.artists = self._generate_realistic_artists()
        self.songs = self._generate_massive_song_database()
        self.users = self._generate_users()
        self.user_sessions = {}
        
        # 더 세밀한 상태 전이 확률
        self.state_transitions = {
            'Home': {'NextSong': 0.45, 'Search': 0.25, 'Browse': 0.15, 'Thumbs Up': 0.03, 'Settings': 0.05, 'Logout': 0.07},
            'NextSong': {'NextSong': 0.65, 'Thumbs Up': 0.15, 'Thumbs Down': 0.08, 'Add to Playlist': 0.07, 'Home': 0.05},
            'Search': {'NextSong': 0.7, 'Browse': 0.2, 'Home': 0.1},
            'Browse': {'NextSong': 0.6, 'Search': 0.25, 'Home': 0.15},
            'Thumbs Up': {'NextSong': 0.8, 'Add to Playlist': 0.15, 'Home': 0.05},
            'Thumbs Down': {'NextSong': 0.9, 'Home': 0.1},
            'Add to Playlist': {'NextSong': 0.7, 'Browse': 0.2, 'Home': 0.1},
            'Settings': {'Home': 0.8, 'Logout': 0.2},
            'Login': {'Home': 1.0},
            'Register': {'Home': 1.0}
        }
        
    def _generate_realistic_artists(self):
        """현실적인 대규모 아티스트 데이터베이스 생성"""
        artists = []
        
        # 메가스타급 (전세계적 인기) - 극소수
        mega_stars = [
            {'name': 'BTS', 'popularity': 100, 'tier': 'mega', 'genres': ['K-Pop', 'Pop']},
            {'name': 'Taylor Swift', 'popularity': 98, 'tier': 'mega', 'genres': ['Pop', 'Country']},
            {'name': 'Drake', 'popularity': 97, 'tier': 'mega', 'genres': ['Hip-Hop', 'R&B']},
            {'name': 'Billie Eilish', 'popularity': 96, 'tier': 'mega', 'genres': ['Pop', 'Alternative']},
            {'name': 'The Weeknd', 'popularity': 95, 'tier': 'mega', 'genres': ['R&B', 'Pop']},
            {'name': 'Ariana Grande', 'popularity': 94, 'tier': 'mega', 'genres': ['Pop', 'R&B']},
            {'name': 'Ed Sheeran', 'popularity': 93, 'tier': 'mega', 'genres': ['Pop', 'Folk']},
            {'name': 'Adele', 'popularity': 92, 'tier': 'mega', 'genres': ['Pop', 'Soul']},
        ]
        
        # 톱스타급 (차트 상위권) - 소수
        top_stars = []
        for i in range(50):
            top_stars.append({
                'name': f'TopStar_{i+1}',
                'popularity': random.randint(75, 91),
                'tier': 'top',
                'genres': random.sample(['Pop', 'Hip-Hop', 'R&B', 'Rock', 'Electronic', 'Country'], 2)
            })
        
        # 유명 아티스트급 (일반적으로 알려진) - 중간 규모
        famous_artists = []
        for i in range(500):
            famous_artists.append({
                'name': f'Famous_{i+1}',
                'popularity': random.randint(50, 74),
                'tier': 'famous',
                'genres': random.sample(['Pop', 'Hip-Hop', 'R&B', 'Rock', 'Electronic', 'Jazz', 'Country', 'Folk'], 2)
            })
        
        # 신인/인디 아티스트급 (대부분) - 대규모
        indie_artists = []
        for i in range(10000):  # 10,000명의 인디 아티스트
            indie_artists.append({
                'name': f'Indie_{i+1}',
                'popularity': random.randint(1, 49),
                'tier': 'indie',
                'genres': random.sample(['Pop', 'Hip-Hop', 'R&B', 'Rock', 'Electronic', 'Jazz', 'Country', 'Folk', 'Alternative', 'Classical'], 1)
            })
        
        artists = mega_stars + top_stars + famous_artists + indie_artists
        print(f"✨ {len(artists):,}명의 아티스트 생성됨")
        return artists
    
    def _generate_massive_song_database(self):
        """현실적인 대규모 곡 데이터베이스 생성 (수백만 곡)"""
        songs = []
        song_id = 1
        
        # 노래 제목 구성 요소들
        title_words = [
            'Love', 'Heart', 'Dream', 'Night', 'Day', 'Fire', 'Rain', 'Dance', 'Time', 'Life',
            'Soul', 'Star', 'Moon', 'Sun', 'Light', 'Dark', 'Blue', 'Red', 'Gold', 'Silver',
            'Beautiful', 'Perfect', 'Forever', 'Never', 'Always', 'Tonight', 'Yesterday', 'Tomorrow',
            'Angel', 'Heaven', 'Paradise', 'Ocean', 'Mountain', 'River', 'Wind', 'Storm',
            'Freedom', 'Journey', 'Adventure', 'Memory', 'Story', 'Magic', 'Wonder', 'Miracle'
        ]
        
        for artist in self.artists:
            # 아티스트 티어에 따른 곡 수 결정
            if artist['tier'] == 'mega':
                num_songs = random.randint(200, 500)  # 메가스타는 많은 곡
            elif artist['tier'] == 'top':
                num_songs = random.randint(80, 200)   # 톱스타는 중간 정도
            elif artist['tier'] == 'famous':
                num_songs = random.randint(20, 80)    # 유명 아티스트는 적당히
            else:  # indie
                num_songs = random.randint(5, 30)     # 인디는 적게
            
            for i in range(num_songs):
                # 곡 제목 생성 (더 다양하게)
                if random.random() < 0.3:
                    title = f"{random.choice(title_words)} {random.choice(title_words)}"
                elif random.random() < 0.5:
                    title = f"{random.choice(title_words)} in {random.choice(['Tokyo', 'Paris', 'LA', 'Seoul', 'London'])}"
                elif random.random() < 0.7:
                    title = f"{random.choice(['My', 'Your', 'Our', 'The'])} {random.choice(title_words)}"
                else:
                    title = f"{random.choice(title_words)} {random.randint(1, 100)}"
                
                # 아티스트 인기도에 따른 곡 인기도 조정
                base_popularity = artist['popularity']
                song_popularity = max(1, min(100, base_popularity + random.randint(-15, 15)))
                
                # 장르는 아티스트의 주 장르에서 선택
                genre = random.choice(artist['genres'])
                
                # 발매 연도 (인기 아티스트는 최근 곡도 많이)
                if artist['tier'] in ['mega', 'top']:
                    year = random.randint(2015, 2024)  # 최근 위주
                elif artist['tier'] == 'famous':
                    year = random.randint(2010, 2024)  # 중간
                else:
                    year = random.randint(2000, 2024)  # 전체 범위
                
                song = {
                    'song_id': f'TR_{song_id:08d}',
                    'title': title,
                    'artist': artist['name'],
                    'artist_popularity': artist['popularity'],
                    'artist_tier': artist['tier'],
                    'album': f"{artist['name']} - Album {random.randint(1, 20)}",
                    'genre': genre,
                    'duration': random.randint(90, 420),  # 1.5분~7분
                    'year': year,
                    'popularity': song_popularity,
                    'play_count': random.randint(100, 10000000),  # 재생 횟수
                    'likes': random.randint(10, 500000),  # 좋아요 수
                }
                songs.append(song)
                song_id += 1
        
        print(f"🎵 {len(songs):,}곡의 음악 데이터베이스 생성됨")
        
        # 인기도별로 정렬하여 차트 효과 구현
        songs.sort(key=lambda x: x['popularity'], reverse=True)
        
        return songs
    
    def _generate_users(self):
        """더 현실적인 사용자 데이터 생성"""
        users = []
        
        # 실제 주요 도시들
        major_cities = [
            'Seoul, KR', 'Tokyo, JP', 'New York, NY', 'Los Angeles, CA', 'London, UK',
            'Paris, FR', 'Berlin, DE', 'Sydney, AU', 'Toronto, CA', 'Amsterdam, NL',
            'Stockholm, SE', 'Copenhagen, DK', 'Mumbai, IN', 'Shanghai, CN', 'São Paulo, BR'
        ]
        
        # 연령대별 음악 취향
        age_genre_preferences = {
            'teen': ['Pop', 'Hip-Hop', 'K-Pop', 'Electronic'],
            'young_adult': ['Pop', 'Hip-Hop', 'R&B', 'Electronic', 'Alternative'],
            'adult': ['Pop', 'Rock', 'R&B', 'Country', 'Folk'],
            'senior': ['Rock', 'Jazz', 'Country', 'Folk', 'Classical']
        }
        
        for i in range(self.num_users):
            # 연령대 결정 (실제 음원 서비스 사용자 분포 반영)
            age_group_rand = random.random()
            if age_group_rand < 0.3:  # 30% - 10대
                age_group = 'teen'
                age = random.randint(13, 19)
            elif age_group_rand < 0.6:  # 30% - 20-30대
                age_group = 'young_adult'
                age = random.randint(20, 35)
            elif age_group_rand < 0.85:  # 25% - 40-50대
                age_group = 'adult'
                age = random.randint(36, 55)
            else:  # 15% - 60대+
                age_group = 'senior'
                age = random.randint(56, 75)
            
            # 유료 사용자 비율 (연령대별로 다름)
            if age_group in ['young_adult', 'adult']:
                is_paid = random.random() < 0.35  # 35%
            elif age_group == 'teen':
                is_paid = random.random() < 0.15  # 15%
            else:  # senior
                is_paid = random.random() < 0.25  # 25%
            
            user = {
                'user_id': i + 1,
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'gender': random.choice(['M', 'F']),
                'age': age,
                'age_group': age_group,
                'level': 'paid' if is_paid else 'free',
                'location': random.choice(major_cities),
                'user_agent': self.fake.user_agent(),
                'registration': self.fake.date_between(start_date='-5y', end_date='today'),
                'favorite_genres': random.sample(age_genre_preferences[age_group], 2),
                'music_taste': random.choice(['mainstream', 'indie', 'mixed']),  # 음악 취향
                'activity_level': random.choice(['low', 'medium', 'high']),  # 활동 수준
            }
            users.append(user)
        
        print(f"👥 {len(users):,}명의 사용자 생성됨")
        return users
    
    def _select_song_intelligently(self, user):
        """사용자의 취향과 실제 음원 서비스 알고리즘을 반영한 노래 선택"""
        
        # 차트 상위권 곡들 (전체의 1%)
        chart_songs = self.songs[:len(self.songs)//100]
        
        # 사용자 취향 맞는 곡들
        preference_songs = [
            s for s in self.songs 
            if s['genre'] in user['favorite_genres']
        ]
        
        # 선택 알고리즘
        selection_rand = random.random()
        
        if user['music_taste'] == 'mainstream':
            if selection_rand < 0.6:  # 60% - 차트 상위곡
                return random.choice(chart_songs) if chart_songs else random.choice(self.songs)
            elif selection_rand < 0.9:  # 30% - 취향 맞는 곡
                return random.choice(preference_songs) if preference_songs else random.choice(self.songs)
            else:  # 10% - 랜덤
                return random.choice(self.songs)
                
        elif user['music_taste'] == 'indie':
            if selection_rand < 0.5:  # 50% - 인디 아티스트 곡
                indie_songs = [s for s in self.songs if s['artist_tier'] == 'indie']
                return random.choice(indie_songs) if indie_songs else random.choice(self.songs)
            elif selection_rand < 0.8:  # 30% - 취향 맞는 곡
                return random.choice(preference_songs) if preference_songs else random.choice(self.songs)
            else:  # 20% - 랜덤
                return random.choice(self.songs)
                
        else:  # mixed
            if selection_rand < 0.3:  # 30% - 차트곡
                return random.choice(chart_songs) if chart_songs else random.choice(self.songs)
            elif selection_rand < 0.6:  # 30% - 취향곡
                return random.choice(preference_songs) if preference_songs else random.choice(self.songs)
            elif selection_rand < 0.8:  # 20% - 인디곡
                indie_songs = [s for s in self.songs if s['artist_tier'] == 'indie']
                return random.choice(indie_songs) if indie_songs else random.choice(self.songs)
            else:  # 20% - 랜덤
                return random.choice(self.songs)
    
    def _get_next_action(self, current_page):
        """현재 페이지에서 다음 액션 결정"""
        if current_page not in self.state_transitions:
            return 'Home'
        
        actions = list(self.state_transitions[current_page].keys())
        probabilities = list(self.state_transitions[current_page].values())
        
        return np.random.choice(actions, p=probabilities)
    
    def _generate_event(self, user, session_id, current_time):
        """단일 이벤트 생성"""
        if user['user_id'] not in self.user_sessions:
            # 새 세션 시작
            self.user_sessions[user['user_id']] = {
                'session_id': session_id,
                'current_page': 'Home',
                'current_song': None,
                'session_start': current_time,
                'item_in_session': 0,
                'songs_played': [],
                'session_duration': random.randint(300, 7200)  # 5분~2시간
            }
        
        session = self.user_sessions[user['user_id']]
        session['item_in_session'] += 1
        
        # 다음 액션 결정
        next_action = self._get_next_action(session['current_page'])
        
        # 이벤트 데이터 생성
        event = {
            'ts': int(current_time.timestamp() * 1000),
            'userId': user['user_id'],
            'sessionId': session['session_id'],
            'page': next_action,
            'auth': 'Logged In',
            'method': self._get_http_method(next_action),
            'status': 200,
            'level': user['level'],
            'itemInSession': session['item_in_session'],
            'location': user['location'],
            'userAgent': user['user_agent'],
            'firstName': user['first_name'],
            'lastName': user['last_name'],
            'gender': user['gender'],
            'age': user['age'],
            'registration': int(datetime.combine(user['registration'], datetime.min.time()).timestamp()) * 1000
        }
        
        # NextSong인 경우 지능적으로 노래 선택
        if next_action == 'NextSong':
            selected_song = self._select_song_intelligently(user)
            session['current_song'] = selected_song
            session['songs_played'].append(selected_song['song_id'])
            
            event.update({
                'artist': selected_song['artist'],
                'song': selected_song['title'],
                'length': selected_song['duration'],
                'artist_tier': selected_song['artist_tier'],
                'song_popularity': selected_song['popularity']
            })
        
        # 세션 상태 업데이트
        session['current_page'] = next_action
        
        # 세션 종료 조건들
        session_elapsed = (current_time - session['session_start']).total_seconds()
        should_logout = (
            next_action == 'Logout' or 
            session_elapsed > session['session_duration'] or
            len(session['songs_played']) > 50
        )
        
        if should_logout:
            del self.user_sessions[user['user_id']]
        
        return event
    
    def _get_http_method(self, action):
        """액션에 따른 HTTP 메소드 결정"""
        method_map = {
            'Thumbs Up': 'PUT', 'Thumbs Down': 'PUT', 'Add to Playlist': 'PUT',
            'Settings': 'GET', 'Search': 'GET', 'Browse': 'GET', 'NextSong': 'PUT',
            'Home': 'GET', 'Login': 'PUT', 'Register': 'PUT', 'Logout': 'PUT'
        }
        return method_map.get(action, 'GET')
    
    def _should_generate_event(self, current_time, user):
        """사용자별, 시간대별 이벤트 생성 확률"""
        hour = current_time.hour
        
        # 기본 시간대별 활동 패턴
        hourly_activity = {
            0: 0.05, 1: 0.02, 2: 0.01, 3: 0.01, 4: 0.01, 5: 0.03,
            6: 0.10, 7: 0.25, 8: 0.40, 9: 0.50, 10: 0.60, 11: 0.70,
            12: 0.80, 13: 0.75, 14: 0.70, 15: 0.75, 16: 0.85, 17: 0.90,
            18: 0.95, 19: 1.00, 20: 0.95, 21: 0.85, 22: 0.70, 23: 0.30
        }
        
        base_probability = hourly_activity.get(hour, 0.5)
        
        # 사용자 활동 수준에 따른 조정
        if user['activity_level'] == 'high':
            base_probability *= 1.5
        elif user['activity_level'] == 'low':
            base_probability *= 0.6
        
        # 연령대별 조정
        if user['age_group'] == 'teen':
            if 15 <= hour <= 23:  # 오후~밤에 더 활발
                base_probability *= 1.3
        elif user['age_group'] == 'young_adult':
            if 18 <= hour <= 24:  # 저녁~밤에 활발
                base_probability *= 1.2
        elif user['age_group'] == 'adult':
            if 8 <= hour <= 18:  # 낮 시간대
                base_probability *= 1.1
        
        # 주말 패턴
        if current_time.weekday() >= 5:  # 주말
            if user['age_group'] in ['teen', 'young_adult']:
                base_probability *= 1.2  # 젊은 층은 주말에 더 활발
            else:
                base_probability *= 0.9  # 나이든 층은 주말에 조금 덜 활발
        
        return random.random() < min(base_probability, 1.0)
    
    def _write_event(self, event):
        """이벤트를 Kafka 또는 JSON 파일로 출력"""
        # Kafka로 전송
        if self.producer:
            self.producer.send(
                self.topic_name,
                key=str(event['userId']),
                value=event
            )
        
        # JSON 파일로 저장
        if self.json_output:
            json.dump(event, self.json_output, ensure_ascii=False)
            self.json_output.write('\n')
            self.json_output.flush()
        
        return event
    
    def start_streaming(self, duration_minutes=None, continuous=False):
        """대규모 현실적 스트리밍 시작"""
        print("\n🎵 대규모 현실적 음악 스트리밍 시뮬레이터 시작")
        print(f"📊 데이터베이스 규모:")
        print(f"   - 아티스트: {len(self.artists):,}명")
        print(f"   - 곡: {len(self.songs):,}곡")
        print(f"   - 사용자: {len(self.users):,}명")
        
        if self.producer:
            print(f"📡 Kafka 브로커: {self.kafka_brokers}")
            print(f"📻 토픽: {self.topic_name}")
        
        if self.output_file:
            print(f"📄 JSON 출력 파일: {self.output_file}")
        
        if continuous:
            print("🔄 연속 모드 (Ctrl+C로 중지)")
        elif duration_minutes:
            print(f"⏱️  {duration_minutes}분 동안 실행")
        
        print("\n--- 샘플 이벤트 (첫 3개) ---")
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes) if duration_minutes else None
        event_count = 0
        session_id = 1
        sample_shown = 0
        
        try:
            while True:
                current_time = datetime.now()
                
                # 종료 조건 확인
                if not continuous and end_time and current_time >= end_time:
                    break
                
                # 현재 시간에 활성화될 사용자들 선택
                active_users = []
                for user in self.users:
                    if self._should_generate_event(current_time, user):
                        active_users.append(user)
                
                # 활성 사용자들 이벤트 생성
                for user in active_users:
                    # 새 세션이 필요한 경우
                    if user['user_id'] not in self.user_sessions:
                        if random.random() < 0.1:  # 10% 확률로 새 세션 시작
                            session_id += 1
                    
                    if user['user_id'] in self.user_sessions or random.random() < 0.1:
                        event = self._generate_event(user, session_id, current_time)
                        self._write_event(event)
                        
                        event_count += 1
                        
                        # 처음 3개 이벤트만 콘솔에 출력
                        if sample_shown < 3:
                            print(json.dumps(event, indent=2, ensure_ascii=False))
                            print("---")
                            sample_shown += 1
                        
                        if event_count % 500 == 0:
                            print(f"📊 전송된 이벤트: {event_count:,}")
                            print(f"🎧 현재 활성 사용자: {len(active_users):,}명")
                
                # 실제적인 간격
                time.sleep(random.uniform(0.1, 1.0))
                
        except KeyboardInterrupt:
            print("\n🛑 사용자가 중지했습니다.")
        
        finally:
            if self.producer:
                self.producer.close()
            if self.json_output:
                self.json_output.close()
            print(f"✅ 총 {event_count:,}개 이벤트 처리 완료")
            if self.output_file:
                print(f"📁 JSON 파일 저장 완료: {self.output_file}")

def main():
    parser = argparse.ArgumentParser(description='대규모 현실적 음악 스트리밍 데이터 시뮬레이터')
    
    # Kafka 설정
    parser.add_argument('--brokers', 
                       help='Kafka 브로커 주소들 (쉼표로 구분)')
    parser.add_argument('--topic', 
                       help='Kafka 토픽 이름')
    
    # 출력 설정
    parser.add_argument('--output', '-o',
                       help='JSON 출력 파일 경로')
    
    # 시뮬레이션 설정
    parser.add_argument('--users', type=int, default=1000,
                       help='시뮬레이션할 사용자 수 (기본값: 1000)')
    parser.add_argument('--duration', type=int,
                       help='실행 시간 (분)')
    parser.add_argument('--continuous', action='store_true',
                       help='연속 실행 모드')
    
    args = parser.parse_args()
    
    # Kafka 설정 파싱
    brokers = args.brokers.split(',') if args.brokers else None
    
    # 출력 방식 검증
    if not brokers and not args.output:
        print("❌ 오류: Kafka 브로커(--brokers) 또는 JSON 출력 파일(--output) 중 하나는 필수입니다.")
        return
    
    print("🚀 대규모 음악 스트리밍 시뮬레이터 초기화 중...")
    
    simulator = RealisticMusicStreamingSimulator(
        kafka_brokers=brokers,
        topic_name=args.topic,
        num_users=args.users,
        output_file=args.output
    )
    
    simulator.start_streaming(
        duration_minutes=args.duration,
        continuous=args.continuous
    )

if __name__ == "__main__":
    main()
