import os
import pandas as pd
import traceback
from datetime import datetime
import time

from api_functions import fetch_player_data, save_raw_data
from data_extractors import extract_player_info, extract_match_data, extract_stats_data

def process_player_data(player_id, save_raw=True):
    """선수 데이터를 가져와서 처리하고 데이터프레임으로 반환"""
    print(f"\n{'='*50}")
    print(f"Processing player ID: {player_id}")
    print(f"{'='*50}")
    
    try:
        # 데이터 가져오기
        player_data = fetch_player_data(player_id)
        if not player_data:
            print(f"Could not fetch data for player {player_id}")
            return None, None, None
        
        # 원본 데이터 저장 (옵션)
        if save_raw:
            save_raw_data(player_data, player_id)
        
        # 데이터 구조 기본 검증
        if not isinstance(player_data, dict):
            print(f"오류: player_data가 딕셔너리가 아닙니다. 타입: {type(player_data)}")
            # 간단한 데이터 내용 출력
            print(f"데이터 미리보기: {str(player_data)[:200]}...")
            return None, None, None
        
        print(f"API 응답 수신 완료. 데이터 크기: {len(str(player_data))} 바이트")
        print(f"데이터에 포함된 키: {list(player_data.keys())}")
        
        # 데이터 추출
        print("\n----- 선수 기본 정보 추출 시작 -----")
        player_info = extract_player_info(player_data)
        if player_info:
            print(f"선수 기본 정보 추출 성공: {player_info['name'] if 'name' in player_info else 'Unknown'}")
        else:
            print("선수 기본 정보 추출 실패")
        
        print("\n----- 경기 데이터 추출 시작 -----")
        match_data = extract_match_data(player_data)
        print(f"경기 데이터 추출 결과: {len(match_data)}개의 경기 정보 찾음")
        
        print("\n----- 통계 데이터 추출 시작 -----")
        stats_data = extract_stats_data(player_data)
        print(f"통계 데이터 추출 결과: {len(stats_data)}개의 통계 정보 찾음")
        
        # 데이터프레임 생성
        player_df = pd.DataFrame([player_info]) if player_info else None
        matches_df = pd.DataFrame(match_data) if match_data else None
        stats_df = pd.DataFrame(stats_data) if stats_data else None
        
        # 결과 요약
        print("\n----- 처리 결과 요약 -----")
        print(f"선수 기본 정보: {'성공' if player_df is not None else '실패'}")
        print(f"경기 데이터: {'성공 - ' + str(len(match_data)) + '개 항목' if matches_df is not None else '없음'}")
        print(f"통계 데이터: {'성공 - ' + str(len(stats_data)) + '개 항목' if stats_df is not None else '없음'}")
        
        print(f"\nSuccessfully processed data for player {player_id}")
        return player_df, matches_df, stats_df
    except Exception as e:
        print(f"\n----- 오류 발생 -----")
        print(f"Error processing player {player_id}: {str(e)}")
        print(f"오류 종류: {type(e).__name__}")
        print(f"상세 오류 정보:\n{traceback.format_exc()}")
        return None, None, None

def save_to_csv(player_dfs=None, matches_dfs=None, stats_dfs=None, base_filename='football_players_data'):
    """수집된 데이터를 CSV 파일에 저장 (기존 데이터 유지하며 업데이트)"""
    try:
        # 파일 경로 설정
        players_file = f"{base_filename}_players.csv"
        matches_file = f"{base_filename}_matches.csv"
        stats_file = f"{base_filename}_stats.csv"
        id_name_file = f"{base_filename}_player_ids.csv"
        
        # 새 데이터프레임 준비
        new_players_df = pd.DataFrame()
        new_matches_df = pd.DataFrame()
        new_stats_df = pd.DataFrame()
        
        # 입력된 데이터프레임이 있으면 합치기
        if player_dfs:
            player_dfs = [df for df in player_dfs if df is not None and not df.empty]
            if player_dfs:
                new_players_df = pd.concat(player_dfs, ignore_index=True)
        
        if matches_dfs:
            matches_dfs = [df for df in matches_dfs if df is not None and not df.empty]
            if matches_dfs:
                new_matches_df = pd.concat(matches_dfs, ignore_index=True)
        
        if stats_dfs:
            stats_dfs = [df for df in stats_dfs if df is not None and not df.empty]
            if stats_dfs:
                new_stats_df = pd.concat(stats_dfs, ignore_index=True)
        
        # 1. 선수 정보 처리 (ID를 기준으로 중복 업데이트)
        if not new_players_df.empty:
            # 기존 파일이 있으면 읽어오기
            if os.path.exists(players_file):
                try:
                    existing_players_df = pd.read_csv(players_file)
                    # 기존 데이터와 새 데이터를 합치기 전 중복 ID 제거
                    existing_ids = set(existing_players_df['id'])
                    new_ids = set(new_players_df['id'])
                    
                    # 중복 ID가 있는지 확인
                    duplicate_ids = existing_ids.intersection(new_ids)
                    
                    if duplicate_ids:
                        print(f"중복된 선수 ID {len(duplicate_ids)}개 발견: 최신 데이터로 업데이트합니다.")
                        # 중복 ID 제거 (새 데이터 우선)
                        existing_players_df = existing_players_df[~existing_players_df['id'].isin(duplicate_ids)]
                    
                    # 기존 데이터에 새 데이터 추가
                    final_players_df = pd.concat([existing_players_df, new_players_df], ignore_index=True)
                    print(f"선수 정보: 기존 {len(existing_players_df)}개 + 신규 {len(new_players_df)}개 = 총 {len(final_players_df)}개")
                except Exception as e:
                    print(f"기존 선수 파일을 읽는 중 오류 발생: {e}. 새 데이터만 저장합니다.")
                    final_players_df = new_players_df
            else:
                final_players_df = new_players_df
                
            # 선수 정보 저장
            final_players_df.to_csv(players_file, index=False)
            print(f"Saved {len(final_players_df)} player records to '{players_file}'")
            
            # 선수 ID 목록 파일 업데이트
            id_name_df = final_players_df[['id', 'name', 'team']].copy()
            id_name_df.to_csv(id_name_file, index=False)
            print(f"Updated player ID list in '{id_name_file}'")
        
        # 2. 경기 데이터 처리 (선수 ID + 경기 ID를 복합키로 사용)
        if not new_matches_df.empty:
            if os.path.exists(matches_file):
                try:
                    existing_matches_df = pd.read_csv(matches_file)
                    # 선수 ID와 경기 ID를 조합한 복합키 생성
                    if 'player_id' in existing_matches_df.columns and 'match_id' in existing_matches_df.columns:
                        existing_matches_df['composite_key'] = existing_matches_df['player_id'].astype(str) + '_' + existing_matches_df['match_id'].astype(str)
                        new_matches_df['composite_key'] = new_matches_df['player_id'].astype(str) + '_' + new_matches_df['match_id'].astype(str)
                        
                        # 중복 제거 (새 데이터 우선)
                        existing_keys = set(existing_matches_df['composite_key'])
                        new_keys = set(new_matches_df['composite_key'])
                        duplicate_keys = existing_keys.intersection(new_keys)
                        
                        if duplicate_keys:
                            print(f"중복된 경기 데이터 {len(duplicate_keys)}개 발견: 최신 데이터로 업데이트합니다.")
                            existing_matches_df = existing_matches_df[~existing_matches_df['composite_key'].isin(duplicate_keys)]
                        
                        # 복합키 컬럼 제거
                        existing_matches_df = existing_matches_df.drop('composite_key', axis=1)
                        new_matches_df = new_matches_df.drop('composite_key', axis=1)
                    
                    final_matches_df = pd.concat([existing_matches_df, new_matches_df], ignore_index=True)
                    print(f"경기 데이터: 기존 {len(existing_matches_df)}개 + 신규 {len(new_matches_df)}개 = 총 {len(final_matches_df)}개")
                except Exception as e:
                    print(f"기존 경기 파일을 읽는 중 오류 발생: {e}. 새 데이터만 저장합니다.")
                    final_matches_df = new_matches_df
            else:
                final_matches_df = new_matches_df
                
            # 경기 데이터 저장
            final_matches_df.to_csv(matches_file, index=False)
            print(f"Saved {len(final_matches_df)} match records to '{matches_file}'")
        
        # 3. 통계 데이터 처리 (선수 ID + 시즌 + 통계 제목을 복합키로 사용)
        if not new_stats_df.empty:
            if os.path.exists(stats_file):
                try:
                    existing_stats_df = pd.read_csv(stats_file)
                    # 선수 ID, 시즌, 통계 제목을 조합한 복합키 생성
                    if all(col in existing_stats_df.columns for col in ['player_id', 'season', 'title']):
                        existing_stats_df['composite_key'] = existing_stats_df['player_id'].astype(str) + '_' + existing_stats_df['season'].astype(str) + '_' + existing_stats_df['title'].astype(str)
                        new_stats_df['composite_key'] = new_stats_df['player_id'].astype(str) + '_' + new_stats_df['season'].astype(str) + '_' + new_stats_df['title'].astype(str)
                        
                        # 중복 제거 (새 데이터 우선)
                        existing_keys = set(existing_stats_df['composite_key'])
                        new_keys = set(new_stats_df['composite_key'])
                        duplicate_keys = existing_keys.intersection(new_keys)
                        
                        if duplicate_keys:
                            print(f"중복된 통계 데이터 {len(duplicate_keys)}개 발견: 최신 데이터로 업데이트합니다.")
                            existing_stats_df = existing_stats_df[~existing_stats_df['composite_key'].isin(duplicate_keys)]
                        
                        # 복합키 컬럼 제거
                        existing_stats_df = existing_stats_df.drop('composite_key', axis=1)
                        new_stats_df = new_stats_df.drop('composite_key', axis=1)
                    
                    final_stats_df = pd.concat([existing_stats_df, new_stats_df], ignore_index=True)
                    print(f"통계 데이터: 기존 {len(existing_stats_df)}개 + 신규 {len(new_stats_df)}개 = 총 {len(final_stats_df)}개")
                except Exception as e:
                    print(f"기존 통계 파일을 읽는 중 오류 발생: {e}. 새 데이터만 저장합니다.")
                    final_stats_df = new_stats_df
            else:
                final_stats_df = new_stats_df
                
            # 통계 데이터 저장
            final_stats_df.to_csv(stats_file, index=False)
            print(f"Saved {len(final_stats_df)} stat records to '{stats_file}'")
        
        print(f"All data successfully saved with base name '{base_filename}'")
        return True
    except Exception as e:
        print(f"Error saving data to CSV: {e}")
        print(f"상세 오류 정보:\n{traceback.format_exc()}")
        return False

def load_processed_ids():
    """이미 처리한 선수 ID와 결과를 불러옴"""
    try:
        if os.path.exists('processed_player_ids.csv'):
            df = pd.read_csv('processed_player_ids.csv')
            # 이미 처리된 ID를 사전 형태로 변환 (ID -> 결과)
            processed_ids = dict(zip(df['player_id'], df['status']))
            print(f"Loaded {len(processed_ids)} previously processed player IDs")
            return processed_ids
        else:
            print("No previously processed IDs found")
            return {}
    except Exception as e:
        print(f"Error loading processed IDs: {e}")
        print(f"상세 오류 정보:\n{traceback.format_exc()}")
        return {}

def save_processed_id(player_id, status):
    """처리한 선수 ID와 결과를 CSV에 저장"""
    try:
        new_row = {'player_id': player_id, 'status': status, 'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        # 파일 잠금을 통한 동시 접근 방지
        max_retries = 5
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                # 기존 파일이 있으면 추가, 없으면 새로 생성
                if os.path.exists('processed_player_ids.csv'):
                    try:
                        df = pd.read_csv('processed_player_ids.csv')
                        
                        # 이미 처리된 ID인지 확인
                        if player_id in df['player_id'].values:
                            # 이미 있는 ID는 업데이트
                            idx = df[df['player_id'] == player_id].index[0]
                            df.at[idx, 'status'] = status
                            df.at[idx, 'timestamp'] = new_row['timestamp']
                        else:
                            # 새 ID 추가
                            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    except pd.errors.EmptyDataError:
                        # 파일이 비어있는 경우
                        print(f"Warning: processed_player_ids.csv exists but is empty. Creating new file.")
                        df = pd.DataFrame([new_row])
                    except Exception as e:
                        print(f"Error reading processed_player_ids.csv: {e}")
                        # 파일이 손상된 경우, 백업 후 새로 생성
                        backup_file = f'processed_player_ids_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                        print(f"Creating backup of existing file as {backup_file} and recreating")
                        try:
                            import shutil
                            shutil.copy2('processed_player_ids.csv', backup_file)
                        except Exception as be:
                            print(f"Failed to create backup: {be}")
                        df = pd.DataFrame([new_row])
                else:
                    # 새 파일 생성
                    df = pd.DataFrame([new_row])
                
                # CSV 저장
                df.to_csv('processed_player_ids.csv', index=False)
                return True
                
            except PermissionError:
                # 파일이 다른 프로세스에 의해 사용 중인 경우
                if attempt < max_retries - 1:
                    print(f"CSV 파일 접근 충돌. {retry_delay}초 후 재시도 ({attempt+1}/{max_retries})...")
                    time.sleep(retry_delay)
                else:
                    # 마지막 시도에서도 실패한 경우 별도 파일에 저장
                    fallback_file = f'processed_player_ids_temp_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                    print(f"CSV 파일 접근이 계속 실패하여 임시 파일 {fallback_file}에 저장합니다.")
                    pd.DataFrame([new_row]).to_csv(fallback_file, index=False)
                    return False
            except Exception as e:
                # 마지막 시도에서도 실패한 경우 별도 파일에 저장
                print(f"ID {player_id} 저장 중 예상치 못한 오류 발생: {str(e)}")
                fallback_file = f'processed_player_ids_error_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                print(f"임시 파일 {fallback_file}에 저장합니다.")
                pd.DataFrame([new_row]).to_csv(fallback_file, index=False)
                break
                
    except Exception as e:
        print(f"Error saving processed ID {player_id}: {e}")
        print(f"상세 오류 정보:\n{traceback.format_exc()}")
        
        # 오류가 발생해도 데이터 손실을 막기 위해 임시 파일에 저장
        try:
            emergency_file = f'player_id_{player_id}_status_{status}.txt'
            with open(emergency_file, 'w') as f:
                f.write(f"player_id: {player_id}\nstatus: {status}\ntimestamp: {new_row['timestamp']}")
            print(f"긴급 백업 파일 {emergency_file}에 정보를 저장했습니다.")
        except:
            print("긴급 백업 파일 생성도 실패했습니다.")
            
        return False

def get_valid_player_ids():
    """유효한 선수 ID만 추출"""
    try:
        if os.path.exists('processed_player_ids.csv'):
            df = pd.read_csv('processed_player_ids.csv')
            valid_ids = df[df['status'].str.startswith('valid')]['player_id'].tolist()
            return valid_ids
        return []
    except Exception as e:
        print(f"유효한 선수 ID 추출 중 오류 발생: {e}")
        print(f"상세 오류 정보:\n{traceback.format_exc()}")
        return []