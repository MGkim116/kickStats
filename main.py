import time

from data_processor import process_player_data, save_to_csv, get_valid_player_ids
from id_explorer import explore_player_ids

def main():
    # 선택할 모드
    print("선택할 모드:")
    print("1. 특정 선수 ID 목록 처리 (CSV 파일 생성)")
    print("2. 선수 ID 범위 탐색 (CSV 파일 생성)")
    print("3. 이미 찾은 유효한 선수만 처리 (CSV 파일 생성)")
    
    mode = input("모드 선택 (1-3): ")
    
    # CSV 파일명 입력 받기
    base_filename = input("저장할 CSV 파일 기본 이름 입력 (기본: football_players_data): ") or "football_players_data"
    
    if mode == "1":
        # 특정 선수 ID 목록 처리
        print("\n선수 ID 목록 입력 방법:")
        print("- 여러 ID는 쉼표로 구분 (예: 212867,312765,1077894)")
        print("- 입력하지 않으면 기본 ID 목록이 사용됩니다")
        player_ids_input = input("\n처리할 선수 ID 입력: ")
        
        player_ids = []
        if player_ids_input.strip():
            # 쉼표로 구분된 ID 처리
            for id_str in player_ids_input.split(","):
                id_str = id_str.strip()
                if id_str.isdigit():
                    player_ids.append(int(id_str))
                else:
                    print(f"경고: '{id_str}'은(는) 유효한 숫자가 아니므로 무시됩니다.")
        
        if not player_ids:
            player_ids = [
                212867,  # 손흥민
                312765,  # 다른 선수
                1077894  # 다른 선수
            ]
            print(f"유효한 ID가 입력되지 않아 기본 ID 목록을 사용합니다: {player_ids}")
        
        # 데이터프레임 컬렉션 초기화
        player_dfs = []
        matches_dfs = []
        stats_dfs = []
        
        for player_id in player_ids:
            player_df, matches_df, stats_df = process_player_data(player_id)
            
            if player_df is not None:
                player_dfs.append(player_df)
            if matches_df is not None:
                matches_dfs.append(matches_df)
            if stats_df is not None:
                stats_dfs.append(stats_df)
                
            time.sleep(2)
        
        # CSV 파일 저장
        save_to_csv(player_dfs, matches_dfs, stats_dfs, base_filename)
    
    elif mode == "2":
        # 선수 ID 범위 탐색
        print("\n탐색할 선수 ID 범위 입력:")
        start_id = int(input("시작 ID 입력 (기본: 212867): ") or "212867")
        end_id = int(input("종료 ID 입력 (기본: 213000): ") or "213000")
        batch_size = int(input("배치 크기 입력 (기본: 10): ") or "10")
        delay = float(input("요청 간 지연 시간(초) 입력 (기본: 2): ") or "2")
        
        explore_player_ids(start_id, end_id, batch_size, delay, base_filename)
    
    elif mode == "3":
        # 유효한 선수만 처리
        valid_ids = get_valid_player_ids()
        if not valid_ids:
            print("유효한 선수 ID가 없습니다. 먼저 ID 탐색을 실행하세요.")
            return
        
        print(f"\n총 {len(valid_ids)}명의 유효한 선수를 찾았습니다.")
        if len(valid_ids) > 10:
            print(f"유효한 선수 ID 일부: {valid_ids[:10]}... (외 {len(valid_ids)-10}개)")
        else:
            print(f"유효한 선수 ID: {valid_ids}")
        
        confirm = input("\n이 선수들의 데이터를 처리하시겠습니까? (y/n): ")
        if confirm.lower() == 'y':
            # 데이터프레임 컬렉션 초기화
            player_dfs = []
            matches_dfs = []
            stats_dfs = []
            
            for i, player_id in enumerate(valid_ids):
                player_df, matches_df, stats_df = process_player_data(player_id)
                
                if player_df is not None:
                    player_dfs.append(player_df)
                if matches_df is not None:
                    matches_dfs.append(matches_df)
                if stats_df is not None:
                    stats_dfs.append(stats_df)
                
                time.sleep(2)
                
                # 중간 저장 (10명마다)
                if (i + 1) % 10 == 0:
                    print(f"중간 결과 저장 중... ({i+1}/{len(valid_ids)})")
                    save_to_csv(player_dfs, matches_dfs, stats_dfs, base_filename)
            
            # 최종 저장
            save_to_csv(player_dfs, matches_dfs, stats_dfs, base_filename)
        else:
            print("처리를 취소했습니다.")
    
    else:
        print("잘못된 모드를 선택했습니다.")
    
    print("작업이 완료되었습니다.")

if __name__ == "__main__":
    main()