import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 제목 설정
st.title('축구 선수 데이터 분석 대시보드')

# 데이터 불러오기
@st.cache_data
def load_data():
    players = pd.read_csv("football_players_data_players.csv")
    matches = pd.read_csv("football_players_data_matches.csv")
    stats = pd.read_csv("football_players_data_stats.csv")
    return players, matches, stats

players_df, matches_df, stats_df = load_data()

# 데이터 미리보기
st.subheader('선수 데이터 미리보기')
st.dataframe(players_df.head())

# 선수 선택 기능
player_list = players_df['name'].tolist()
selected_player = st.selectbox('선수 선택:', player_list)

# 선택된 선수 데이터 표시
if selected_player:
    player_id = players_df[players_df['name'] == selected_player]['id'].values[0]
    st.subheader(f'{selected_player}의 경기 데이터')
    player_matches = matches_df[matches_df['player_id'] == player_id]
    st.dataframe(player_matches)
    
    # 통계 차트
    if not player_matches.empty:
        st.subheader('득점과 어시스트')
        fig, ax = plt.subplots(figsize=(10, 5))
        player_matches.plot(x='match_date', y=['goals', 'assists'], kind='bar', ax=ax)
        st.pyplot(fig)