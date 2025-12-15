import numpy as np
import pandas as pd

import streamlit as st
from streamlit_folium import st_folium
import matplotlib.pyplot as plt

from rec import Recommender
from geocoder import Geocoder
from knowledge_ver3 import KnowledgeGraph
import folium
import hashlib

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

[수정 2025-12-09]
issue: 지도 출력 event 무한 호출.
solve: maptest.py 참고하여 코드 재작성.

issue: 추천 기사 url 하이퍼링크
solve: dashboard.py 참고하여 코드 수정.

issue: 파이차트, 지도 중앙정렬
solve: column을 세분화하여 중앙 정렬.

[수정 2025-12-10]
issue: knowledge graph 시각화
solve: knowledge.py 연동

[수정 2025-12-11]
issue: KG2개 뉴스에 포함되는 키워드만 표시 / 추천 뉴스 상단 클릭한 뉴스 표시
solve: knowledge.py 수정 / 코드 수정
detail: 
    - knowledge graph 버전 업데이트(knowledge_ver3.py)
    - 토글: "기사 더보기" >> "기사 목록 펼치기"
    - 클릭된 기사 재클릭하여 취소 시, 초기화면으로 복귀

"""


# =========================
# DB 설정
# =========================
DB = dict(
    host="localhost",
    database="nvisiaDb",
    user="postgres",
    password="postgres1202",
    port=5432,
)


# =========================
# 공용 커넥터 / 헬퍼
# =========================
def get_psql_conn():
    """간단 쿼리용 psycopg2 커넥션 (글로벌 캐시 X, 매번 열고 닫기)"""
    conn = psycopg2.connect(
        host=DB["host"],
        database=DB["database"],
        user=DB["user"],
        password=DB["password"],
        port=DB["port"],
        options="-c client_encoding=UTF8 -c lc_messages=C",
    )
    return conn

@st.cache_data
def load_all_articles():
    conn = get_psql_conn()
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

    df = pd.DataFrame(rows)
    if "publish_date" in df.columns:
        df["publish_date"] = df["publish_date"].astype(str).str[:10]
    return df

def split_event_locs(event_loc_str: str):
    """'평양시, 함경북도 청진시' → ['평양시', '함경북도 청진시']"""
    if not event_loc_str:
        return []
    return [p.strip() for p in event_loc_str.split(",") if p.strip()]


# =========================
# Streamlit 세팅
# =========================
st.set_page_config(page_title="News Data Dashboard", layout="wide")
st.title("NVISIA: North-Korea Vision & Insights by SIA")

# Matplotlib 한글 폰트
plt.rc("font", family="Malgun Gothic")
plt.rc("axes", unicode_minus=False)

# =========================
# 객체 호출
# =========================
@st.cache_resource
def get_rec():
    return Recommender(**DB)

@st.cache_resource
def get_geo():
    return Geocoder(**DB)

rec = get_rec()
geo = get_geo()

if "selected_id" not in st.session_state:
    st.session_state["selected_id"] = None

if "expanded" not in st.session_state:
    st.session_state.expanded = False


# =========================
# 데이터 로드
# =========================
df = load_all_articles()


# =========================
# 레이아웃
#   top_left  : 파이차트
#   top_right : 추천 기사 테이블
#   bottom_left : 전체 기사 테이블
#   bottom_right: 지도
# =========================
top_left, top_middle, top_right = st.columns([1, 1, 1])

st.divider()

def toggle_expanded():
    st.session_state.expanded = not st.session_state.expanded

st.button(
    "기사 목록 펼치기" if not st.session_state.expanded else "되돌리기",
    on_click=toggle_expanded,
)

table_height = 600 if st.session_state.expanded else 250

bottom_left, bottom_right = st.columns([2, 1])


# =========================
# bottom_left: 전체 기사 목록 + 선택 (row 클릭 방식)
# =========================
with bottom_left:
    st.subheader("전체 기사 목록")

    df_display = df[['id', 'title', 'summary', 'publish_date', 'category']].copy()
    st.caption(f"총 {len(df_display)}개 기사 - 더 많은 기사를 보려면 스크롤하세요.")

    event = st.dataframe(
        df_display,
        width="stretch",
        height=table_height,
        selection_mode="single-row",
        on_select="rerun",
        key="article_table",
    )

    if event.selection.rows:
        idx = event.selection.rows[0]
        st.session_state["selected_id"] = df_display.iloc[idx]["id"]

    else:
        st.session_state["selected_id"] = None

selected_id = st.session_state.get("selected_id")


# =========================
# top: 추천 기사 테이블 + 파이차트 세팅
# =========================
rec_list = []
rec_ids = []
rec_df_view = pd.DataFrame()

chart_df = df.copy()
chart_title = "전체 뉴스 카테고리"

if selected_id is not None:
    rec_list = rec.get_similar_articles(selected_id, k=10)
    rec_ids = [r["id"] for r in rec_list]

    if rec_list:
        df_rec = df[df["id"].isin(rec_ids)].copy()
        df_rec.set_index("id", inplace=True)

        rows = []

        base_row_all = df[df["id"] == selected_id]
        if not base_row_all.empty:
            base_row = base_row_all.iloc[0]

            base_title = (base_row.get("title", "") or "")
            base_summary = (base_row.get("summary", "") or "")

            rows.append(
                {
                    "id": selected_id,
                    "title": base_title[:50] + ("..." if len(base_title) > 80 else ""),
                    "summary": base_summary[:50] + ("..." if len(base_summary) > 50 else ""),
                    "category": base_row.get("category", ""),
                    "publish_date": base_row.get("publish_date", ""),
                    "url": base_row.get("url", ""),
                }
            )

        for r in rec_list:
            rid = r["id"]
            base = df_rec.loc[rid] if rid in df_rec.index else {}

            title = (base.get("title", r.get("title", "")) or "")
            summary = (base.get("summary", "") or "")
            category = base.get("category", r.get("category", ""))
            publish_date = base.get("publish_date", r.get("publish_date", ""))
            url = base.get("url", r.get("url", ""))

            rows.append(
                {
                    "id": rid,
                    "title": title[:50] + ("..." if len(title) > 80 else ""),
                    "summary": summary[:50] + ("..." if len(summary) > 50 else ""),
                    "category": category,
                    "publish_date": publish_date,
                    "url": url,
                }
            )

        rec_df_view = pd.DataFrame(rows)

        if not rec_df_view.empty:
            chart_df = rec_df_view
            chart_title = "추천 뉴스 카테고리"


# =========================
# top_right: 추천 기사 테이블
# =========================
with top_right:
    if selected_id is not None:
        st.subheader(f"관련 추천 뉴스 (기준: {selected_id})")
    else:
        st.subheader("관련 추천 뉴스")

    if not rec_df_view.empty:

        column_config = {}
        if 'url' in rec_df_view.columns:
            column_config["url"] = st.column_config.LinkColumn(
                "Link",
                display_text="Open Article"
            )
   
        st.dataframe(
            rec_df_view[['url', 'id', 'title', 'publish_date']],
            width="stretch",
            hide_index=True,
            height=300,
            column_config=column_config,
        )
    else:
        if selected_id is None:
            st.info("아래 목록에서 기사를 선택하면 추천 뉴스가 표시됩니다.")
        else:
            st.info("추천 기사가 없습니다.")


# =========================
# top_middle: knowledge graph
# =========================
with top_middle:
    st.subheader("Knowledge Graph")

    if rec_list:
        try:
            know = KnowledgeGraph(rec_list)
            fig = know.get_figure()
            st.pyplot(fig, width = "content")

        except Exception as e:
            st.error(f"그래프 생성 중 오류가 발생했습니다: {e}")
    else:
        st.info("추천 기사의 키워드들을 바탕으로 그래프가 생성됩니다.")

# =========================
# top_left: 파이차트
# =========================
with top_left:
    if "category" in chart_df.columns:
        st.subheader(chart_title)
        category_counts = chart_df["category"].value_counts()

        if not category_counts.empty:

            # left_spacer, center_col, right_spacer = st.columns([1, 3, 1])

            # # 파이차트 가운데 위치
            # with center_col:

            def autopct_filter(pct):
                return ('%1.1f%%' % pct) if pct > 5 else ''
            
            fig, ax = plt.subplots(figsize=(1.7, 1.7))
            # 레이블 바깥쪽, 회전 없음
            wedges, texts, autotexts = ax.pie(
                category_counts,
                labels=category_counts.index,
                autopct=autopct_filter,
                startangle=90,
                textprops={"fontsize": 4},
            )               

            # 파이 내부 퍼센트 글자 크기 작게
            for autotext in autotexts:
                autotext.set_fontsize(4)

            ax.axis("equal")
            st.pyplot(fig, width="content")
        else:
            st.info("입력된 데이터가 없습니다. 데이터를 추가해주세요.")
    else:
        st.info("카테고리 로드 중 오류가 발생했습니다.")


# =========================
# bottom_right: 지도
#   - 선택된 기사 한 건만 표시
# =========================
with bottom_right:

    if selected_id:
        df_sel = df[df["id"] == selected_id].copy()

        locs = set()
        for loc_str in df_sel["event_loc"].fillna(""):
            for loc in split_event_locs(loc_str):
                locs.add(loc)
        locs = sorted(locs)

        if locs:
            geo_dict = geo.get_geometry(locs)

            center = (39.0, 127.0)
            m = folium.Map(location=center, zoom_start=7)

            def color_from_name(name):
                h = hashlib.md5(name.encode("utf-8")).hexdigest()[:6]
                return f"#{h}"

            for loc, geom in geo_dict.items():
                feature = {
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {"event_loc": loc},
                }

                folium.GeoJson(
                    feature,
                    name=loc,
                    tooltip=folium.Tooltip(loc),
                    style_function=lambda x, loc_name=loc: {
                        "fillColor": color_from_name(loc_name),
                        "color": "black",
                        "weight": 1,
                        "fillOpacity": 0.6,
                    },
                    highlight_function=lambda x: {
                        "weight": 3,
                        "color": "yellow",
                        "fillOpacity": 0.8,
                    },
                ).add_to(m)

            left_spacer, center_col, right_spacer = st.columns([0.5, 3, 0.5])
            with center_col:
                st_folium(m, width="100%", height=400)

        else:
            st.info("선택된 기사에 위치 정보가 없습니다.")
    else:
        st.info("위치를 조회하고자 하는 기사를 선택해주세요.")
