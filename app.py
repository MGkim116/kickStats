import streamlit as st
import pandas as pd

# ✅ 1. 배경 이미지 + 버튼 스타일 CSS
page_bg_css = '''
<style>
[data-testid="stAppViewContainer"] {
    background-image: url("https://search.pstatic.net/common/?src=http%3A%2F%2Fcafefiles.naver.net%2FMjAxODA3MDRfMjQx%2FMDAxNTMwNzEzODYzMTU5.mnXVzi63HeSCj2IeNFpjaDtTR3I7vXuvO5PBYNAPI0Ug.PoQ4v0LB6dKgWkj0N9DCO7YmYnuGirMwTJ7W-DhdTJMg.JPEG.halamadridista%2FexternalFile.jpg&type=sc960_832");
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
}

[data-testid="stHeader"] {
    background: rgba(0,0,0,0);
}

div.stButton > button {
    background-color: #4CAF50;
    color: white;
    font-size: 18px;
    padding: 10px 24px;
    border-radius: 8px;
    border: none;
    transition: 0.3s;
}

div.stButton > button:hover {
    background-color: #45a049;
}
</style>
'''
st.markdown(page_bg_css, unsafe_allow_html=True)

# ✅ 2. 제목
st.title('⚽ 축구 선수 데이터 분석 대시보드')

# ✅ 3. 데이터 불러오기
@st.cache_data
def load_data():
    players = pd.read_csv("football_players_data_players.csv")
    matches = pd.read_csv("football_players_data_matches.csv")
    stats = pd.read_csv("football_players_data_stats.csv")
    return players, matches, stats

players_df, matches_df, stats_df = load_data()

# ✅ 4. 선수 데이터 미리보기
st.subheader('📋 선수 데이터 미리보기')
st.dataframe(players_df.head())

# ✅ 5. 선수 선택
player_list = players_df['name'].tolist()
selected_player = st.selectbox('🔍 선수 선택:', player_list)

# ✅ 6. 선택된 선수의 경기 데이터 + 버튼 기능
if selected_player:
    player_id = players_df[players_df['name'] == selected_player]['id'].values[0]

    if st.button(f"📊 {selected_player}의 경기 데이터 보기"):
        player_matches = matches_df[matches_df['player_id'] == player_id]
        st.subheader(f'📌 {selected_player}의 경기 데이터')
        st.dataframe(player_matches)

        # ✅ 다운로드 버튼
        csv = player_matches.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="⬇️ 선수 경기 데이터 다운로드",
            data=csv,
            file_name=f"{selected_player}_matches.csv",
            mime='text/csv'
        )

    if st.button(f"📈 {selected_player}의 주요 스탯 보기"):
        player_stats = stats_df[stats_df['player_id'] == player_id]
        st.subheader(f"📍 {selected_player}의 주요 스탯")
        st.dataframe(player_stats)

# ✅ 외부 링크 버튼 예시
st.link_button("🌐 FIFA 공식 홈페이지 가기", "https://www.fifa.com")
