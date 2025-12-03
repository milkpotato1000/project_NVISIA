
# #==================================

# import streamlit as st
# import streamlit.components.v1 as components

# from geocoder import Geocoder
# from rec import Recommender

# DB = dict(
#     host="localhost",
#     database="nvisiaDb",
#     user="postgres",
#     password="postgres1202",
#     port=5432,
# )

# # 세션에 DB 연결 객체를 한 번만 만들기
# if "geo" not in st.session_state:
#     st.session_state["geo"] = Geocoder(**DB)
# if "rec" not in st.session_state:
#     st.session_state["rec"] = Recommender(**DB)

# geo = st.session_state["geo"]
# rec = st.session_state["rec"]

# st.title("북한 뉴스 지도 테스트")

# # 지도 HTML을 세션에 저장
# if "map_html" not in st.session_state:
#     st.session_state["map_html"] = None

# # 기사 ID 입력 (문자열)
# click_id = st.text_input("기사 ID 입력 (예: spnews_100196)", "spnews_100196")

# # 버튼을 눌렀을 때만 새 지도 생성
# if st.button("지도 보기") and click_id:
#     m = geo.get_map(click_id, rec, k=10)
#     st.session_state["map_html"] = m._repr_html_()   # folium 지도 → HTML 문자열로 저장

# # 저장된 HTML이 있으면 항상 그대로 표시 (재생성 X)
# if st.session_state["map_html"]:
#     components.html(st.session_state["map_html"], height=600)

# # 종료 시점에만 수동으로 close 하고 싶으면, 별도 버튼 만들어도 됨
# # if st.button("DB 연결 닫기"):
# #     geo.close()
# #     rec.close()

# #==================================

import os
import json
from typing import List

import psycopg2
from psycopg2.extras import RealDictCursor

import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
import folium
import random

from geocoder import Geocoder
from rec import Recommender


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


@st.cache_data(show_spinner=False)
def load_all_articles() -> pd.DataFrame:
    """하단에 보여줄 전체 기사 목록 로드 (id 기준 내림차순)"""
    conn = get_psql_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT
            id,
            category,
            summary,
            publish_date,
            url,
            event_loc
        FROM spnews_summary
        ORDER BY id DESC
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    df = pd.DataFrame(rows)
    # 문자열 변환
    if "publish_date" in df.columns:
        df["publish_date"] = df["publish_date"].astype(str).str[:10]
    return df


def split_event_locs(event_loc_str: str) -> List[str]:
    """'평양시, 함경북도 청진시' → ['평양시', '함경북도 청진시']"""
    if not event_loc_str:
        return []
    return [p.strip() for p in event_loc_str.split(",") if p.strip()]


# =========================
# Streamlit 세팅
# =========================
st.set_page_config(layout="wide", page_title="북한 뉴스 지도 대시보드")

st.title("북한 뉴스 지도 대시보드 (테스트 버전)")

# Geocoder / Recommender 객체는 세션에 1번만 생성
if "geo" not in st.session_state:
    st.session_state["geo"] = Geocoder(**DB)
if "rec" not in st.session_state:
    st.session_state["rec"] = Recommender(**DB)

geo: Geocoder = st.session_state["geo"]
rec: Recommender = st.session_state["rec"]

# 지도 클릭으로 선택된 event_loc 저장 (하단 필터용 + 추천 리스트 하이라이트용)
if "selected_loc" not in st.session_state:
    st.session_state["selected_loc"] = None

# 현재 선택된 기사 id
if "selected_id" not in st.session_state:
    st.session_state["selected_id"] = None

# =========================
# 데이터 로드
# =========================
df_all = load_all_articles()

# =========================
# 레이아웃: 상단(지도 + 추천리스트) / 하단(전체 기사 테이블)
# =========================
top_left, top_right = st.columns([2, 1])
bottom = st.container()


# =========================
# 1) 하단: 전체 기사 테이블 + 선택
# =========================
with bottom:
    st.subheader("전체 기사 목록")

    # 지도에서 event_loc 클릭했을 때 하단 필터링
    filter_loc = st.session_state["selected_loc"]

    df_view = df_all.copy()
    if filter_loc:
        df_view = df_view[
            df_view["event_loc"].fillna("").str.contains(filter_loc, na=False)
        ]

    # 보여줄 컬럼만 선택
    df_display = df_view[["id", "category", "summary", "publish_date", "url"]]

    st.caption(
        f"총 {len(df_display)}개 기사"
        + (f" (필터: event_loc에 '{filter_loc}' 포함)" if filter_loc else "")
    )

    # 선택된 기사 id (현재 필터된 테이블 기준)
    id_options = df_display["id"].tolist()
    if id_options:
        # 기본 선택: 이전 선택 id가 있으면 그대로, 없으면 첫 번째
        if st.session_state["selected_id"] in id_options:
            default_index = id_options.index(st.session_state["selected_id"])
        else:
            default_index = 0

        selected_id = st.selectbox(
            "상단 지도 / 추천에 사용할 기사 선택",
            options=id_options,
            index=default_index,
            key="select_id_box",
        )
        st.session_state["selected_id"] = selected_id
    else:
        st.warning("현재 필터 조건에 해당하는 기사가 없습니다.")
        selected_id = None

    # 전체 테이블 표시 (필터 적용된)
    st.dataframe(
        df_display,
        use_container_width=True,
        height=300,
    )

    # 필터 초기화 버튼
    cols = st.columns([1, 9])
    with cols[0]:
        if st.button("필터 초기화"):
            st.session_state["selected_loc"] = None


# =========================
# 2) 상단 우측: 추천 기사 리스트
# =========================
with top_right:
    st.subheader("추천 기사 (Top 10)")

    rec_list = []
    rec_ids = []

    if selected_id:
        rec_list = rec.get_similar_articles(selected_id, k=10)
        rec_ids = [r["id"] for r in rec_list]

        # 선택 기사 + 추천 기사들을 df_all과 조인해서 event_loc 등 가져오기
        df_rec = df_all[df_all["id"].isin([selected_id] + rec_ids)].copy()
        df_rec.set_index("id", inplace=True)

        # 표시는 간단한 리스트/테이블 형태로
        rows = []
        # 첫 줄: 선택된 기사
        if selected_id in df_rec.index:
            base_row = df_rec.loc[selected_id]
            rows.append(
                {
                    "id": selected_id,
                    "type": "선택",
                    "category": base_row.get("category", ""),
                    "publish_date": base_row.get("publish_date", ""),
                    "event_loc": base_row.get("event_loc", ""),
                    "title": base_row.get("summary", "")[:80] + "...",
                    "url": base_row.get("url", ""),
                }
            )

        # 다음 줄: 추천 기사들
        for r in rec_list:
            rid = r["id"]
            base = df_rec.loc[rid] if rid in df_rec.index else {}
            rows.append(
                {
                    "id": rid,
                    "type": "추천",
                    "category": base.get("category", r.get("category", "")),
                    "publish_date": base.get("publish_date", r.get("publish_date", "")),
                    "event_loc": base.get("event_loc", ""),
                    "title": base.get("summary", "")[:80] + "...",
                    "url": base.get("url", r.get("url", "")),
                }
            )

        df_rec_view = pd.DataFrame(rows)

        # 지도에서 선택된 event_loc이 있으면 해당 event_loc을 가진 기사들을 볼드 느낌으로 표시
        highlight_loc = st.session_state["selected_loc"]

        def highlight_row(row):
            if highlight_loc and highlight_loc in (row.get("event_loc") or ""):
                return ["font-weight: bold; background-color: #fce4ec"] * len(row)
            return [""] * len(row)

        if not df_rec_view.empty:
            st.dataframe(
                df_rec_view[
                    ["type", "id", "category", "publish_date", "event_loc", "title", "url"]
                ].style.apply(highlight_row, axis=1),
                use_container_width=True,
                height=400,
            )
        else:
            st.info("추천 기사를 찾을 수 없습니다.")
    else:
        st.info("하단에서 기사를 선택하면 추천 결과가 표시됩니다.")


# =========================
# 3) 상단 좌측: 지도
# =========================
with top_left:
    st.subheader("지도")

    if selected_id:
        # 지도에 표시할 기사 id 목록: 선택된 기사 + 추천 10개
        ids_for_map = [selected_id] + rec_ids
        df_map = df_all[df_all["id"].isin(ids_for_map)].copy()

        # 이 기사들에서 event_loc 추출
        locs = set()
        for loc_str in df_map["event_loc"].fillna(""):
            for loc in split_event_locs(loc_str):
                locs.add(loc)

        locs = sorted(locs)

        if locs:
            # PostGIS 에서 geometry 가져오기
            geo_dict = geo.get_geometry(locs)

            # folium 지도 생성
            center = (39.0, 127.0)
            m = folium.Map(location=center, zoom_start=7)

            # event_loc → 이 지역을 가진 기사 id 리스트 매핑
            loc_to_article_ids = {}
            for _, row in df_map.iterrows():
                this_id = row["id"]
                for loc in split_event_locs(row["event_loc"] or ""):
                    loc_to_article_ids.setdefault(loc, []).append(this_id)

            # 색상 고정을 위해 이름 기반 해시
            def color_from_name(name: str) -> str:
                import hashlib

                h = hashlib.md5(name.encode("utf-8")).hexdigest()[:6]
                return f"#{h}"

            for loc, geom in geo_dict.items():
                feature = {
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "event_loc": loc,
                        "article_ids": loc_to_article_ids.get(loc, []),
                    },
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

            # Streamlit에 지도 표시 + 클릭 이벤트 받기
            map_state = st_folium(m, width=800, height=500)

            # 5) 지도에서 구역 클릭 시: 해당 event_loc으로 하단 테이블 필터
            if map_state and map_state.get("last_active_drawing"):
                props = map_state["last_active_drawing"].get("properties", {})
                clicked_loc = props.get("event_loc")
                if clicked_loc:
                    st.session_state["selected_loc"] = clicked_loc
                    st.info(f"선택된 지역: {clicked_loc} (하단 기사 목록 필터링됨)")
        else:
            st.info("선택된 기사/추천 기사에 event_loc 정보가 없습니다.")
    else:
        st.info("하단에서 기사를 선택하면 지도에 해당 지역이 표시됩니다.")
