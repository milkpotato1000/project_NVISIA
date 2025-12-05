import numpy as np
import pandas as pd
import ast

# 시각화
import streamlit as st
from streamlit_folium import st_folium
import matplotlib.pyplot as plt

# LLM
from LLMtoDatabase import LLMtoDatabase

# 추천, 지도
from rec import Recommender
from geocoder import Geocoder
import folium
import random

# postgres
import psycopg2
from psycopg2.extras import RealDictCursor

NOTES = """

[작성 2025-12-03]
dashboard.py 내용 수정. postgres 연동 목적.

동작 순서:
    1) 기사 원문 csv 입력
    2) LLMtoDatabase 통해 summary, embedding, categorizing 진행
    3) Recommender 통해 관련 기사 k개 추천
    4) Geocoder 통해 지도에 위치 표시

[수정 2025-12-03]
- url 하이퍼링크 적용.

[수정 2025-12-04]
geocoder 통해 사용자가 클릭한 기사의 event_loc을 지도에 표시(추천 기사의 event_loc은 표시하지 않음).
Recommender, Geocoder 에서 모두 활용할 selected_id 객체 생성하였음.

"""

# ========================= #
DB = dict(
    host="localhost",
    database="nvisiaDb",
    user="postgres",
    password="postgres1202",
    port=5432,
)

@st.cache_resource
def get_rec():
    return Recommender(**DB)

@st.cache_resource
def get_geo():
    return Geocoder(**DB)

rec = get_rec()
geo = get_geo()
# ========================= #


# Matplotlib 한글 폰트 설정 (Windows)
plt.rc('font', family='Malgun Gothic')
plt.rc('axes', unicode_minus=False)

st.set_page_config(page_title="News Data Dashboard", layout="wide")

st.title("NVISIA: North-Korea Vision & Insights by SIA")

@st.cache_data
def load_all_articles():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT
            id,
            title,
            summary,
            publish_date,
            category,
            event_loc,
            url
        FROM spnews_summary
        ORDER BY id DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows)

# 데이터 로드
df = load_all_articles()

# publish_date 기준 내림차순 정렬 (최신 날짜가 먼저)
if not df.empty and 'publish_date' in df.columns:
    df = df.sort_values('publish_date', ascending=False).reset_index(drop=True)

if df.empty:
    st.warning("No data to display.")
else:

    # 확장 상태 초기화
    if "expanded" not in st.session_state:
        st.session_state.expanded = False
    if "selected_id" not in st.session_state:
        st.session_state.selected_id = None
    if "map_obj" not in st.session_state:
        st.session_state.map_obj = None
    if "rec_df" not in st.session_state:
        st.session_state.rec_df = None

    # 상단 섹션을 위한 1:2 비율 컬럼 생성
    col1, col2 = st.columns([1, 2])

    # 동적 콘텐츠를 위한 플레이스홀더
    with col1:
        chart_container = st.empty()

    with col2:
        rec_container = st.empty()

    st.divider()

    # 확장 토글 버튼
    def toggle_expanded():
        st.session_state.expanded = not st.session_state.expanded

    st.button(
        "Expand table" if not st.session_state.expanded else "Collapse table",
        on_click=toggle_expanded
    )
    
    # 높이 결정
    table_height = 600 if st.session_state.expanded else 250

    # 레이아웃 생성: 왼쪽은 데이터프레임, 오른쪽은 지도
    df_col, map_col = st.columns([2, 1])

    with df_col:
        # 스크롤 가능한 컨테이너에 전체 데이터프레임 표시
        # 행 선택 활성화
        display_columns = ['id', 'title', 'summary', 'publish_date', 'category']

        # 데이터프레임에 존재하는 컬럼만 포함
        display_columns = [col for col in display_columns if col in df.columns]

        event = st.dataframe(
            df[display_columns], 
            height=table_height, 
            use_container_width=True, 
            on_select="rerun", 
            selection_mode="single-row"
        )
        st.caption(f"Showing {len(df)} rows – scroll to view the rest.")

    # rerun에서 사용자가 선택한 id (streamlit 무한 호출로 인해 코드 수정)
    current_selected_id = None

    if len(event.selection.rows) > 0:
        selected_idx = event.selection.rows[0]
        current_selected_id = df.iloc[selected_idx]["id"]

    # 선택이 바뀐 경우에만 DB 호출
    if current_selected_id is not None and current_selected_id != st.session_state.selected_id:
        st.session_state.selected_id = current_selected_id

        if current_selected_id is not None:
            # 지도 새로 생성
            st.session_state.map_obj = geo.get_map_single(current_selected_id)

            # 추천 기사 새로 조회
            rec_list = rec.get_similar_articles(current_selected_id, k=10)
            rec_df = pd.DataFrame(rec_list)
            if not rec_df.empty:
                rec_df = rec_df.merge(
                    df[['id', 'summary']],
                    on='id',
                    how='left'
                )
            st.session_state.rec_df = rec_df

    with map_col:
        if st.session_state.map_obj is not None:
            st_folium(st.session_state.map_obj, width=300, height=400, key="map")
        else:
            st.info("위치를 조회하고자 하는 기사를 선택해주세요.")

    # 차트(col1)에 사용할 데이터 결정 및 col2 업데이트
    if st.session_state.selected_id is not None and st.session_state.rec_df is not None:
        rec_df = st.session_state.rec_df
        chart_df = rec_df
        chart_title = "추천 뉴스 카테고리"
        
        # col2에 추천 데이터 테이블 표시
        with rec_container.container():
            st.subheader(f"관련 추천 뉴스 (기준: {st.session_state.selected_id})")
            if not rec_df.empty:
                # 긴 텍스트 축약을 위한 복사본 생성
                cols_for_display = ['id', 'title', 'summary', 'category', 'publish_date']
                cols_for_display = [c for c in cols_for_display if c in rec_df.columns]
                display_df = rec_df[cols_for_display].copy()

                if 'summary' in display_df.columns:
                    display_df['summary'] = display_df['summary'].apply(lambda x: x[:50] + '...' if isinstance(x, str) and len(x) > 50 else x)
                    
                # 정렬 가능한 컬럼을 위해 st.dataframe 사용
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    height=300
                )
            else:
                st.info("No recommended data available.")
    else:
        # 선택된 행 없음: 전체 데이터를 차트에 사용
        chart_df = df
        chart_title = "전체 뉴스 카테고리"
        
        # col2에 안내 메시지 표시
        with rec_container.container():
            st.info("아래 목록에서 기사를 선택하면 추천 뉴스가 표시됩니다.")

    # col1에 파이 차트 그리기
    with chart_container.container():
        if 'category' in chart_df.columns:
            st.subheader(chart_title)
            category_counts = chart_df['category'].value_counts()
            
            if not category_counts.empty:
                def autopct_filter(pct):
                    return ('%1.1f%%' % pct) if pct > 5 else ''
                    
                # 작은 크기
                fig, ax = plt.subplots(figsize=(1.7, 1.7)) 
                # 레이블 바깥쪽, 회전 없음
                wedges, texts, autotexts = ax.pie(
                    category_counts, 
                    labels=category_counts.index, 
                    autopct=autopct_filter, 
                    startangle=90, 
                    textprops={'fontsize': 4}
                )
                
                # 파이 내부 퍼센트 글자 크기 작게
                for autotext in autotexts:
                    autotext.set_fontsize(4)
                    
                ax.axis('equal')
                st.pyplot(fig, use_container_width=False)
            else:
                st.info("No category data to display.")
