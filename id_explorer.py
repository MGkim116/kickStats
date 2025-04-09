import time
import traceback

from data_processor import process_player_data, save_to_csv, load_processed_ids, save_processed_id
from api_functions import fetch_player_data

def explore_player_ids(start_id, end_id, batch_size=10, delay=2, base_filename='football_players_data'):
    """주어진 범위 내의 선수 ID를 탐색하고 유효한 ID 처리"""
    print(f"\n{'*'*70}")
    print(f"Starting exploration of player IDs from {start_id} to {end_id}")
    print(f"{'*'*70}\n")
    
    # 이미 처리한 ID 불러오기
    processed_ids = load_processed_ids()
    print(f"Found {len(processed_ids)} previously processed IDs")
    
    # 데이터프레임 컬렉션 초기화
    player_dfs = []
    matches_dfs = []
    stats_dfs = []
    batch_counter = 0
    valid_counter = 0
    invalid_counter = 0
    error_counter = 0
    
    try:
        # 배치 단위로 처리
        for current_id in range(start_id, end_id + 1):
            try:
                print(f"\n{'>'*30} 현재 ID: {current_id} {'<'*30}")
                
                # 이미 처리된 ID면 스킵
                if current_id in processed_ids:
                    status = processed_ids[current_id]
                    print(f"Player ID {current_id} already processed with status: {status}")
                    
                    # 통계를 위해 카운터 증가
                    if status.startswith("valid"):
                        valid_counter += 1
                        if status == "valid_error":
                            error_counter += 1
                    else:
                        invalid_counter += 1
                    
                    continue
                
                print(f"Exploring player ID: {current_id}")
                
                # 데이터 가져오기 시도
                player_data = fetch_player_data(current_id)
                
                # 응답 기본 검증
                if player_data:
                    print(f"API 응답 성공. 응답 크기: {len(str(player_data))} 바이트")
                    
                    # API 응답에 기본 정보가 포함되어 있는지 확인
                    if 'id' in player_data and 'name' in player_data:
                        print(f"✅ Valid player found: ID {current_id}, Name: {player_data['name']}")
                        
                        # 유효한 선수 데이터 처리
                        player_df, matches_df, stats_df = process_player_data(current_id)
                        
                        # 데이터프레임 컬렉션에 추가
                        if player_df is not None:
                            player_dfs.append(player_df)
                            print(f"Player info added to collection")
                        else:
                            print(f"⚠️ 경고: 선수 정보 추출 실패")
                            
                        if matches_df is not None:
                            matches_dfs.append(matches_df)
                            print(f"Match data added to collection: {len(matches_df)} records")
                        
                        if stats_df is not None:
                            stats_dfs.append(stats_df)
                            print(f"Stats data added to collection: {len(stats_df)} records")
                        
                        # 처리 결과 저장
                        status = "valid_processed" if player_df is not None else "valid_error"
                        save_processed_id(current_id, status)
                        
                        # 통계 업데이트
                        valid_counter += 1
                        if status == "valid_error":
                            error_counter += 1
                            
                        batch_counter += 1
                    else:
                        print(f"⚠️ 응답은 성공했지만 유효한 선수 데이터가 아닙니다")
                        print(f"응답에 포함된 키: {list(player_data.keys())}")
                        print(f"❌ Invalid player ID: {current_id}")
                        save_processed_id(current_id, "invalid")
                        invalid_counter += 1
                else:
                    print(f"❌ Invalid player ID: {current_id}")
                    save_processed_id(current_id, "invalid")
                    invalid_counter += 1
                
                # 진행 상황 업데이트
                print(f"\n진행 상황: {current_id - start_id + 1}/{end_id - start_id + 1} 완료 " + 
                     f"({round((current_id - start_id + 1)/(end_id - start_id + 1)*100, 1)}%)")
                print(f"유효한 선수: {valid_counter}, 유효하지 않은 ID: {invalid_counter}, 오류 발생: {error_counter}")
                
                # API 요청 간 지연
                print(f"다음 요청까지 {delay}초 대기 중...")
                time.sleep(delay)
                
                # 배치 처리 완료 시 중간 결과 저장
                if batch_counter > 0 and batch_counter % batch_size == 0:
                    print(f"\n{'='*50}")
                    print(f"Completed batch of {batch_size} valid players. Last processed ID: {current_id}")
                    print(f"Saving interim results to CSV files...")
                    save_to_csv(player_dfs, matches_dfs, stats_dfs, base_filename)
                    print(f"Taking a short break before next batch...")
                    print(f"{'='*50}")
                    time.sleep(delay * 2)  # 배치 간 추가 지연
            
            except Exception as e:
                print(f"\n{'!'*50}")
                print(f"ID {current_id} 처리 중 예상치 못한 오류 발생:")
                print(f"오류 유형: {type(e).__name__}")
                print(f"오류 내용: {str(e)}")
                print(f"상세 오류 정보:\n{traceback.format_exc()}")
                print(f"{'!'*50}")
                
                # 오류 상태 저장
                save_processed_id(current_id, "processing_error")
                error_counter += 1
                
                # 짧은 지연 후 계속
                time.sleep(1)
                continue
        
        # 최종 결과 저장
        if player_dfs or matches_dfs or stats_dfs:
            print("\n최종 결과 저장 중...")
            save_to_csv(player_dfs, matches_dfs, stats_dfs, base_filename)
        
        print(f"\n{'*'*70}")
        print(f"Exploration completed for ID range {start_id} to {end_id}")
        print(f"처리 결과 요약:")
        print(f"- 검사한 ID 개수: {end_id - start_id + 1}")
        print(f"- 유효한 선수 ID: {valid_counter}")
        print(f"- 유효하지 않은 ID: {invalid_counter}")
        print(f"- 오류 발생: {error_counter}")
        print(f"{'*'*70}")
        
    except KeyboardInterrupt:
        print("\n\n프로그램이 사용자에 의해 중단되었습니다.")
        print("지금까지의 결과를 저장합니다...\n")
        
        # 중단 시점까지의 결과 저장
        if player_dfs or matches_dfs or stats_dfs:
            save_to_csv(player_dfs, matches_dfs, stats_dfs, base_filename)
            
        # 요약 정보 출력
        print(f"처리 요약:")
        print(f"- 마지막으로 처리한 ID: {current_id}")
        print(f"- 유효한 선수 ID: {valid_counter}")
        print(f"- 유효하지 않은 ID: {invalid_counter}")
        print(f"- 오류 발생: {error_counter}")
        
    except Exception as e:
        print(f"\n{'!'*70}")
        print(f"탐색 과정에서 치명적인 오류 발생:")
        print(f"오류 유형: {type(e).__name__}")
        print(f"오류 내용: {str(e)}")
        print(f"상세 오류 정보:\n{traceback.format_exc()}")
        print(f"{'!'*70}")
        
        # 오류 발생 시점까지의 결과 저장
        if player_dfs or matches_dfs or stats_dfs:
            print("\n오류 발생 시점까지의 결과를 저장합니다...")
            save_to_csv(player_dfs, matches_dfs, stats_dfs, base_filename)