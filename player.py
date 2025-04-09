import requests
import json
import csv
import time
import os

def is_valid_player(data):
    """playerData에 선수 핵심 정보가 있는지 확인"""
    return (
        isinstance(data, dict)
        and data.get("name")
        and data.get("primaryTeam")
    )

def fetch_player(player_id):
    url = f"https://www.fotmob.com/api/playerData?id={player_id}"
    headers = {
        'User-Agent': 'Mozilla/5.0',
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        return res.json()
    except:
        return None

def main():
    start_id = 212866
    end_id = 312866
    save_interval = 100

    os.makedirs("output", exist_ok=True)

    found_players = []
    not_found_ids = []

    for pid in range(start_id, end_id + 1):
        data = fetch_player(pid)

        if data and is_valid_player(data):
            info = {
                "id": data.get("id"),
                "name": data.get("name"),
                "team": data.get("primaryTeam", {}).get("teamName"),
                "team_id": data.get("primaryTeam", {}).get("teamId"),
                "position": data.get("positionDescription", {}).get("primaryPosition", {}).get("label"),
                "country": None,
            }
            # country 추출
            for item in data.get("playerInformation", []):
                if item.get("title", "").lower() == "country":
                    info["country"] = item.get("value", {}).get("fallback")
            found_players.append(info)
            print(f"✅ Found: {info['name']} ({info['id']})")
        else:
            not_found_ids.append(pid)
            print(f"❌ Not found: {pid}")

        # 저장 주기
        if pid % save_interval == 0:
            with open("output/players_found.csv", "a", newline='', encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["id", "name", "team", "team_id", "position", "country"])
                if f.tell() == 0: writer.writeheader()
                writer.writerows(found_players)
            with open("output/not_found_ids.txt", "a", encoding="utf-8") as f:
                for nf in not_found_ids:
                    f.write(f"{nf}\n")
            found_players.clear()
            not_found_ids.clear()
            print(f"💾 Saved up to ID {pid}")

        time.sleep(1.5)  # 차단 방지용 딜레이

    print("🎉 Done!")

if __name__ == "__main__":
    main()