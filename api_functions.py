import requests
import json
import time
import os
import traceback

def fetch_player_data(player_id):
    """선수 ID를 사용하여 FotMob API에서 데이터를 수집하는 함수"""
    url = f"https://www.fotmob.com/api/playerData?id={player_id}"
    
    headers = {
        'sec-ch-ua-platform': 'macOS',
        'Referer': f'https://www.fotmob.com/players/{player_id}',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'x-mas': 'eyJib2R5Ijp7InVybCI6Ii9hcGkvcGxheWVyRGF0YT9pZD0yMTI4NjciLCJjb2RlIjoxNzQ0MTk4NDEwNjQxLCJmb28iOiJwcm9kdWN0aW9uOjYxNDNkZWVhMmM2ODdkNzBhOTU5MTZlY2YzOTIzZTk1ZmRlYjMzYWQtdW5kZWZpbmVkIn0sInNpZ25hdHVyZSI6IkYwMEMxNzhBMENGMDYzQjQ5ODc5N0I2MUEwMUM4QkFBIn0='
    }
    
    # 최대 재시도 횟수 설정
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            print(f"API 요청 시도 중... (시도 {attempt + 1}/{max_retries})")
            response = requests.get(url, headers=headers, timeout=15)  # 타임아웃 추가
            
            # 응답 상태 코드 확인
            if response.status_code == 200:
                print(f"API 요청 성공: 상태 코드 {response.status_code}")
                try:
                    data = response.json()
                    return data
                except json.JSONDecodeError as je:
                    print(f"JSON 파싱 오류: {je}")
                    print(f"응답 내용 미리보기: {response.text[:200]}...")
                    if attempt == max_retries - 1:
                        return None
            elif response.status_code == 404:
                print(f"선수를 찾을 수 없음 (404): ID {player_id}는 유효하지 않은 것 같습니다.")
                return None  # 404는 재시도하지 않음
            elif response.status_code == 429:
                print(f"요청 한도 초과 (429): 속도 제한에 도달했습니다. 더 긴 대기 시간이 필요합니다.")
                # 429 오류는 더 오래 대기
                if attempt < max_retries - 1:
                    longer_delay = retry_delay * 3
                    print(f"{longer_delay}초 후에 재시도합니다...")
                    time.sleep(longer_delay)
                else:
                    return None
            else:
                print(f"API 오류: 상태 코드 {response.status_code}")
                # 다른 오류는 일반적인 지연으로 재시도
                if attempt < max_retries - 1:
                    print(f"{retry_delay}초 후에 재시도합니다...")
                    time.sleep(retry_delay)
                else:
                    return None
        
        except requests.exceptions.Timeout:
            print(f"API 요청 타임아웃")
            if attempt < max_retries - 1:
                print(f"{retry_delay}초 후에 재시도합니다...")
                time.sleep(retry_delay)
            else:
                return None
        
        except requests.exceptions.ConnectionError:
            print(f"연결 오류: 네트워크 문제가 발생했습니다.")
            if attempt < max_retries - 1:
                print(f"{retry_delay}초 후에 재시도합니다...")
                time.sleep(retry_delay)
            else:
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"API 요청 오류: {e}")
            if attempt < max_retries - 1:
                print(f"{retry_delay}초 후에 재시도합니다...")
                time.sleep(retry_delay)
            else:
                return None
    
    print(f"모든 재시도 실패: player ID {player_id}에 대한 데이터를 가져올 수 없습니다.")
    return None

def save_raw_data(data, player_id):
    """수집한 원본 데이터를 파일로 저장"""
    os.makedirs('raw_data', exist_ok=True)
    
    with open(f'raw_data/player_{player_id}.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    print(f"Raw data for player {player_id} saved successfully")