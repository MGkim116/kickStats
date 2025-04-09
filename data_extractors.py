import traceback

def extract_player_info(data):
    """선수의 기본 정보 추출"""
    try:
        # 기본 정보 추출 전 데이터 구조 확인
        if 'id' not in data:
            print(f"키 오류: 'id' 필드가 데이터에 없습니다.")
            return None
        
        # 필수 필드 체크와 상세 로깅
        required_fields = [
            ('name', None), 
            ('birthDate', 'utcTime'), 
            ('primaryTeam', 'teamName'), 
            ('primaryTeam', 'teamId'),
            ('positionDescription', 'primaryPosition', 'label'),
            ('isCaptain', None)
        ]
        
        # 필수 필드 체크
        for field_path in required_fields:
            temp_data = data
            path_str = ""
            
            for i, field in enumerate(field_path):
                if field is None:
                    continue
                    
                path_str += f"['{field}']" if i == 0 else f".{field}"
                
                if field not in temp_data:
                    print(f"키 오류: '{path_str}' 경로의 '{field}' 필드가 데이터에 없습니다.")
                    print(f"사용 가능한 키: {list(temp_data.keys())}")
                    if isinstance(temp_data, dict) and len(temp_data) < 10:
                        print(f"현재 데이터 내용: {temp_data}")
                    return None
                
                temp_data = temp_data[field]
        
        # 모든 필수 필드가 확인되면 선수 정보 생성
        player_info = {
            'id': data['id'],
            'name': data['name'],
            'birth_date': data.get('birthDate', {}).get('utcTime', None),
            'team': data.get('primaryTeam', {}).get('teamName', None),
            'team_id': data.get('primaryTeam', {}).get('teamId', None),
            'position': data.get('positionDescription', {}).get('primaryPosition', {}).get('label', None),
            'is_captain': data.get('isCaptain', False),
            'country': None,
            'height': None,
            'shirt': None,
            'age': None,
            'preferred_foot': None,
            'market_value': None
        }
        
        # 선수 세부 정보가 있는지 확인
        if 'playerInformation' not in data:
            print("경고: 'playerInformation' 필드가 데이터에 없습니다.")
        else:
            # 선수 세부 정보 추출
            for item in data['playerInformation']:
                if 'title' not in item or 'value' not in item:
                    print(f"경고: playerInformation 항목에 'title' 또는 'value' 필드가 없습니다: {item}")
                    continue
                    
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
        print(f"Error extracting player info - KeyError: {e}")
        return None
    except Exception as e:
        print(f"Error extracting player info - Unexpected error: {str(e)}")
        print(f"상세 오류 정보: {traceback.format_exc()}")
        return None

def extract_match_data(data):
    """선수의 경기 데이터 추출"""
    try:
        matches = []
        
        # 기본 확인
        if 'id' not in data:
            print("경고: 선수 ID가 데이터에 없습니다")
            return []
            
        player_id = data['id']
        
        # recentMatches 필드 확인
        if 'recentMatches' not in data:
            print(f"선수 ID {player_id}에 대한 'recentMatches' 데이터가 없습니다")
            return []
            
        if not data['recentMatches']:
            print(f"선수 ID {player_id}의 최근 경기 데이터가 비어있습니다")
            return []
        
        # 각 경기 데이터 처리
        for i, match in enumerate(data['recentMatches']):
            try:
                # 필수 필드 확인
                required_fields = [
                    'id', 'matchDate', 'leagueId', 'leagueName', 'teamId', 'teamName',
                    'opponentTeamId', 'opponentTeamName', 'isHomeTeam', 'homeScore', 'awayScore',
                    'minutesPlayed', 'goals', 'assists', 'yellowCards', 'redCards'
                ]
                
                # 누락된 필드가 있는지 확인
                missing_fields = [field for field in required_fields if field not in match]
                if missing_fields:
                    print(f"경기 #{i}에 누락된 필드가 있습니다: {missing_fields}")
                    # 누락된 필드가 있지만 계속 진행 (None으로 채움)
                
                # matchDate가 있는지 확인하고 utcTime 확인
                if 'matchDate' not in match:
                    print(f"경기 #{i}에 'matchDate' 필드가 없습니다")
                    match_date = None
                elif 'utcTime' not in match['matchDate']:
                    print(f"경기 #{i}의 'matchDate'에 'utcTime' 필드가 없습니다")
                    match_date = None
                else:
                    match_date = match['matchDate']['utcTime']
                
                # 안전하게 필드 접근
                match_info = {
                    'player_id': player_id,
                    'match_id': match.get('id'),
                    'match_date': match_date,
                    'league_id': match.get('leagueId'),
                    'league_name': match.get('leagueName'),
                    'team_id': match.get('teamId'),
                    'team_name': match.get('teamName'),
                    'opponent_team_id': match.get('opponentTeamId'),
                    'opponent_team_name': match.get('opponentTeamName'),
                    'is_home': match.get('isHomeTeam'),
                    'home_score': match.get('homeScore'),
                    'away_score': match.get('awayScore'),
                    'minutes_played': match.get('minutesPlayed'),
                    'goals': match.get('goals'),
                    'assists': match.get('assists'),
                    'yellow_cards': match.get('yellowCards'),
                    'red_cards': match.get('redCards'),
                    'rating': match.get('ratingProps', {}).get('num', None)
                }
                matches.append(match_info)
            except Exception as e:
                print(f"경기 #{i} 데이터 추출 중 오류 발생: {str(e)}")
                # 오류가 있지만 계속 진행
            
        return matches
    except KeyError as e:
        print(f"Error extracting match data - KeyError: {e}")
        return []
    except Exception as e:
        print(f"Error extracting match data - Unexpected error: {str(e)}")
        print(f"상세 오류 정보: {traceback.format_exc()}")
        return []

def extract_stats_data(data):
    """선수의 리그 통계 데이터 추출"""
    try:
        stats = []
        
        # 기본 확인
        if 'id' not in data:
            print("경고: 선수 ID가 데이터에 없습니다")
            return []
            
        player_id = data['id']
        
        # mainLeague 필드 확인
        if 'mainLeague' not in data:
            print(f"선수 ID {player_id}에 대한 'mainLeague' 데이터가 없습니다")
            return []
            
        # stats 필드 확인
        if 'stats' not in data['mainLeague']:
            print(f"선수 ID {player_id}의 'mainLeague'에 'stats' 데이터가 없습니다")
            return []
            
        # 필수 필드 확인
        if 'leagueId' not in data['mainLeague']:
            print(f"선수 ID {player_id}의 'mainLeague'에 'leagueId' 필드가 없습니다")
            print(f"사용 가능한 키: {list(data['mainLeague'].keys())}")
            league_id = None
        else:
            league_id = data['mainLeague']['leagueId']
            
        if 'leagueName' not in data['mainLeague']:
            print(f"선수 ID {player_id}의 'mainLeague'에 'leagueName' 필드가 없습니다")
            league_name = None
        else:
            league_name = data['mainLeague']['leagueName']
            
        if 'season' not in data['mainLeague']:
            print(f"선수 ID {player_id}의 'mainLeague'에 'season' 필드가 없습니다")
            season = None
        else:
            season = data['mainLeague']['season']
        
        # 각 통계 데이터 처리
        for i, stat in enumerate(data['mainLeague']['stats']):
            try:
                # 필수 필드 확인
                if 'title' not in stat:
                    print(f"통계 #{i}에 'title' 필드가 없습니다")
                    continue
                    
                if 'value' not in stat:
                    print(f"통계 '{stat.get('title', f'#{i}')}' 에 'value' 필드가 없습니다")
                    continue
                
                stat_info = {
                    'player_id': player_id,
                    'league_id': league_id,
                    'league_name': league_name,
                    'season': season,
                    'title': stat['title'],
                    'value': stat['value']
                }
                stats.append(stat_info)
            except Exception as e:
                print(f"통계 #{i} 데이터 추출 중 오류 발생: {str(e)}")
                # 오류가 있지만 계속 진행
                
        return stats
    except KeyError as e:
        print(f"Error extracting stats data - KeyError: {e}")
        return []
    except Exception as e:
        print(f"Error extracting stats data - Unexpected error: {str(e)}")
        print(f"상세 오류 정보: {traceback.format_exc()}")
        return []