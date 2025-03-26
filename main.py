import requests
import json
import time
import os
import pandas as pd
from datetime import datetime

def fetch_player_data(player_id):
    """선수 ID를 사용하여 FotMob API에서 데이터를 수집하는 함수"""
    url = f"https://www.fotmob.com/api/playerData?id={player_id}"
    
    headers = {
        'sec-ch-ua-platform': 'macOS',
        'Referer': f'https://www.fotmob.com/players/{player_id}',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'x-mas': 'eyJib2R5Ijp7InVybCI6Ii9hcGkvcGxheWVyRGF0YT9pZD0yMTI4NjciLCJjb2RlIjoxNzQyOTg4ODU2NDY5LCJmb28iOiJwcm9kdWN0aW9uOmQ0ZjNiMzliOGQ2ZDBiMmE0MmNiYmQ2ZWM2Mjg5NzkyMjY4ODI2NTItdW5kZWZpbmVkIn0sInNpZ25hdHVyZSI6Ijc3NUE3QkJCRkNEQjlGMzREREIxODQ0NzM1ODk2RENFIn0='
    }
    
    # 참고: 실제로는 x-mas 헤더가 필요할 수 있습니다
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # HTTP 오류 체크
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for player ID {player_id}: {e}")
        return None

def save_raw_data(data, player_id):
    """수집한 원본 데이터를 파일로 저장"""
    os.makedirs('raw_data', exist_ok=True)
    
    with open(f'raw_data/player_{player_id}.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    print(f"Raw data for player {player_id} saved successfully")

def extract_player_info(data):
    """선수의 기본 정보 추출"""
    try:
        player_info = {
            'id': data['id'],
            'name': data['name'],
            'birth_date': data['birthDate']['utcTime'],
            'team': data['primaryTeam']['teamName'],
            'team_id': data['primaryTeam']['teamId'],
            'position': data['positionDescription']['primaryPosition']['label'],
            'is_captain': data['isCaptain'],
            'country': None,
            'height': None,
            'shirt': None,
            'age': None,
            'preferred_foot': None,
            'market_value': None
        }
        
        # 선수 세부 정보 추출
        for item in data['playerInformation']:
            title = item['title'].lower()
            if title == 'country':
                player_info['country'] = item['value'].get('fallback')
            elif title == 'height':
                player_info['height'] = item['value'].get('numberValue')
            elif title == 'shirt':
                player_info['shirt'] = item['value'].get('numberValue')
            elif title == 'age':
                player_info['age'] = item['value'].get('numberValue')
            elif title == 'preferred foot':
                player_info['preferred_foot'] = item['value'].get('key')
            elif title == 'market value':
                player_info['market_value'] = item['value'].get('numberValue')
        
        return player_info
    except KeyError as e:
        print(f"Error extracting player info: {e}")
        return None

def extract_match_data(data):
    """선수의 경기 데이터 추출"""
    try:
        matches = []
        player_id = data['id']
        
        for match in data['recentMatches']:
            match_info = {
                'player_id': player_id,
                'match_id': match['id'],
                'match_date': match['matchDate']['utcTime'],
                'league_id': match['leagueId'],
                'league_name': match['leagueName'],
                'team_id': match['teamId'],
                'team_name': match['teamName'],
                'opponent_team_id': match['opponentTeamId'],
                'opponent_team_name': match['opponentTeamName'],
                'is_home': match['isHomeTeam'],
                'home_score': match['homeScore'],
                'away_score': match['awayScore'],
                'minutes_played': match['minutesPlayed'],
                'goals': match['goals'],
                'assists': match['assists'],
                'yellow_cards': match['yellowCards'],
                'red_cards': match['redCards'],
                'rating': match.get('ratingProps', {}).get('num', None)
            }
            matches.append(match_info)
            
        return matches
    except KeyError as e:
        print(f"Error extracting match data: {e}")
        return []

def extract_stats_data(data):
    """선수의 리그 통계 데이터 추출"""
    try:
        stats = []
        player_id = data['id']
        
        if 'mainLeague' in data and 'stats' in data['mainLeague']:
            league_id = data['mainLeague']['leagueId']
            league_name = data['mainLeague']['leagueName']
            season = data['mainLeague']['season']
            
            for stat in data['mainLeague']['stats']:
                stat_info = {
                    'player_id': player_id,
                    'league_id': league_id,
                    'league_name': league_name,
                    'season': season,
                    'title': stat['title'],
                    'value': stat['value']
                }
                stats.append(stat_info)
                
        return stats
    except KeyError as e:
        print(f"Error extracting stats data: {e}")
        return []

def process_player_to_csv(player_id, save_raw=True):
    """선수 데이터를 가져와서 처리하고 CSV 파일로 저장"""
    print(f"Processing player ID: {player_id}")
    
    # 데이터 가져오기
    player_data = fetch_player_data(player_id)
    if not player_data:
        print(f"Could not fetch data for player {player_id}")
        return False
    
    # 원본 데이터 저장 (옵션)
    if save_raw:
        save_raw_data(player_data, player_id)
    
    try:
        # 데이터 추출
        player_info = extract_player_info(player_data)
        match_data = extract_match_data(player_data)
        stats_data = extract_stats_data(player_data)
        
        # CSV 폴더 생성
        os.makedirs('csv_data', exist_ok=True)
        
        # 데이터프레임 생성 및 CSV 저장
        if player_info:
            player_df = pd.DataFrame([player_info])
            player_csv_path = f'csv_data/player_{player_id}.csv'
            player_df.to_csv(player_csv_path, index=False)
            print(f"Player info saved to {player_csv_path}")
        
        if match_data:
            matches_df = pd.DataFrame(match_data)
            matches_csv_path = f'csv_data/matches_{player_id}.csv'
            matches_df.to_csv(matches_csv_path, index=False)
            print(f"Match data saved to {matches_csv_path}")
        
        if stats_data:
            stats_df = pd.DataFrame(stats_data)
            stats_csv_path = f'csv_data/stats_{player_id}.csv'
            stats_df.to_csv(stats_csv_path, index=False)
            print(f"Stats data saved to {stats_csv_path}")
            
        print(f"Successfully processed and stored data for player {player_id}")
        return True
    except Exception as e:
        print(f"Error processing player {player_id}: {e}")
        return False

def combine_csv_files():
    """모든 선수의 CSV 파일을 종류별로 합쳐서 단일 CSV 파일로 만듦"""
    try:
        # CSV 파일 목록 가져오기
        player_files = [f for f in os.listdir('csv_data') if f.startswith('player_')]
        match_files = [f for f in os.listdir('csv_data') if f.startswith('matches_')]
        stats_files = [f for f in os.listdir('csv_data') if f.startswith('stats_')]
        
        # 선수 데이터 합치기
        player_dfs = [pd.read_csv(os.path.join('csv_data', f)) for f in player_files]
        if player_dfs:
            all_players_df = pd.concat(player_dfs, ignore_index=True)
            all_players_df.to_csv('all_players.csv', index=False)
            print(f"Combined {len(player_dfs)} player files into all_players.csv")
        
        # 경기 데이터 합치기
        match_dfs = [pd.read_csv(os.path.join('csv_data', f)) for f in match_files]
        if match_dfs:
            all_matches_df = pd.concat(match_dfs, ignore_index=True)
            all_matches_df.to_csv('all_matches.csv', index=False)
            print(f"Combined {len(match_dfs)} match files into all_matches.csv")
        
        # 통계 데이터 합치기
        stats_dfs = [pd.read_csv(os.path.join('csv_data', f)) for f in stats_files]
        if stats_dfs:
            all_stats_df = pd.concat(stats_dfs, ignore_index=True)
            all_stats_df.to_csv('all_stats.csv', index=False)
            print(f"Combined {len(stats_dfs)} stats files into all_stats.csv")
            
        return True
    except Exception as e:
        print(f"Error combining CSV files: {e}")
        return False

def main():
    # 처리할 선수 ID 목록
    player_ids = [
        212867,  # 손흥민
        312765,
        1077894
        # 원하는 선수 ID 추가
    ]
    
    # 각 선수 처리 및 CSV 저장
    for player_id in player_ids:
        process_player_to_csv(player_id)
        # API 요청 사이에 약간의 지연 추가
        time.sleep(2)
    
    # 모든 CSV 파일 합치기
    combine_csv_files()
    
    print("Data collection and processing completed")

if __name__ == "__main__":
    main()