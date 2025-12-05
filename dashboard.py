# Streamlit 대시보드 - summary_df_final.csv 시각화
# 유사도 기반 동적 추천 시스템 포함 (URL 링크 기능 추가)

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import ast

# Matplotlib 한글 폰트 설정 (Windows)
plt.rc('font', family='Malgun Gothic')
plt.rc('axes', unicode_minus=False)

st.set_page_config(page_title="News Data Dashboard", layout="wide")

st.title("NVISIA: North-Korea Vision & Insights by SIA")

# CSV 로드 (인코딩 문제 처리)
DATA_PATH = "data/test_df_embedding.csv"

@st.cache_data
def load_data(path):
    try:
        df = pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(path, encoding="cp949")
        except Exception as e:
            st.error(f"Failed to read CSV: {e}")
            return pd.DataFrame()
    return df

@st.cache_data
def prepare_embeddings(df):
    """유사도 계산을 위한 임베딩 준비"""
    if 'embedding' not in df.columns:
        st.error("No 'embedding' column found in the data!")
        return None, None
    
    # 문자열 형태의 리스트를 실제 배열로 변환
    df_copy = df.copy()
    df_copy["embedding"] = df_copy["embedding"].apply(ast.literal_eval)
    embeddings = np.array(df_copy["embedding"].to_list())
    
    # 빠른 검색을 위한 id-index 매핑 생성
    id_to_index = {id_val: idx for idx, id_val in enumerate(df_copy["id"])}
    
    return embeddings, id_to_index

def get_similar_articles(df, embeddings, id_to_index, click_id, k=10):
    """
    코사인 유사도 기반으로 가장 유사한 k개의 기사 반환
    
    Args:
        df: 기사 데이터가 담긴 DataFrame
        embeddings: 임베딩 벡터 numpy 배열
        id_to_index: id를 index로 매핑하는 딕셔너리
        click_id: 클릭된 기사의 ID
        k: 반환할 추천 기사 개수
    
    Returns:
        추천 기사가 담긴 DataFrame
    """
    if click_id not in id_to_index:
        st.warning(f"Article ID {click_id} not found in embeddings!")
        return pd.DataFrame()
    
    click_idx = id_to_index[click_id]
    click_vec = embeddings[click_idx]
    
    # 코사인 유사도 계산
    sims = embeddings @ click_vec
    
    # 클릭된 기사 자체는 제외
    sims[click_idx] = -1.0
    
    # 상위 k개 인덱스 추출
    top_idx = np.argsort(-sims)[:k]
    
    # 추천 기사 반환
    return df.iloc[top_idx]

# 데이터 로드
df = load_data(DATA_PATH)

# publish_date 기준 내림차순 정렬 (최신 날짜가 먼저)
if not df.empty and 'publish_date' in df.columns:
    df = df.sort_values('publish_date', ascending=False).reset_index(drop=True)

if df.empty:
    st.warning("No data to display.")
else:
    # 추천 시스템을 위한 임베딩 준비
    embeddings, id_to_index = prepare_embeddings(df)
    
    # 상단 섹션을 위한 1:2 비율 컬럼 생성
    col1, col2 = st.columns([1, 2])

    # 동적 콘텐츠를 위한 플레이스홀더
    with col1:
        chart_container = st.empty()
    
    with col2:
        rec_container = st.empty()

    st.divider()

    # 확장 상태 초기화
    if "expanded" not in st.session_state:
        st.session_state.expanded = False
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
        # embedding 컬럼 제외 및 컬럼 순서 지정
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
    
    with map_col:
        try:
            from PIL import Image
            map_image = Image.open("data/map.png") # 지도파일 로딩 경로 
            st.image(map_image, caption="지리 정보 (향후 PostgreSQL 연동 예정)", width=int(map_image.width * 0.7))
        except FileNotFoundError:
            st.info("지도 이미지를 찾을 수 없습니다. (data/map.png)")
        except Exception as e:
            st.error(f"지도 이미지 로드 오류: {e}")


    # 차트에 사용할 데이터 결정 및 col2 업데이트
    if len(event.selection.rows) > 0 and embeddings is not None:
        # 행 선택됨: 유사도 기반 추천 가져오기
        selected_idx = event.selection.rows[0]
        selected_id = df.iloc[selected_idx]["id"]
        
        # 추천 기사 가져오기
        rec_df = get_similar_articles(df, embeddings, id_to_index, selected_id, k=10)
        
        chart_df = rec_df
        chart_title = "추천 뉴스 카테고리"
        
        # col2에 추천 데이터 테이블 표시
        with rec_container.container():
            st.subheader(f"관련 추천 뉴스 (기준: {df.iloc[selected_idx]['id']})")
            if not rec_df.empty:
                # 긴 텍스트 축약을 위한 복사본 생성
                # url 컬럼이 있는지 확인하고 포함 (순서 변경: url을 맨 앞으로)
                cols_to_display = ['id', 'title', 'summary', 'category', 'event_date']
                if 'url' in rec_df.columns:
                    cols_to_display.insert(0, 'url')
                
                display_df = rec_df[cols_to_display].copy()
                
                if 'summary' in display_df.columns:
                    display_df['summary'] = display_df['summary'].apply(lambda x: x[:50] + '...' if isinstance(x, str) and len(x) > 50 else x)
                
                # 컬럼 설정: url 컬럼을 LinkColumn으로 설정
                column_config = {}
                if 'url' in display_df.columns:
                    column_config["url"] = st.column_config.LinkColumn(
                        "Link",
                        display_text="Open Article"
                    )

                # 정렬 가능한 컬럼을 위해 st.dataframe 사용
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    height=300,
                    column_config=column_config
                )
            else:
                st.info("No recommended data available.")
    else:
        # 선택된 행 없음: 전체 데이터를 차트에 사용
        chart_df = df
        chart_title = "전체 뉴스 카테고리"
        
        # col2에 안내 메시지 표시
        rec_container.info("아래 목록에서 기사를 선택하면 추천 뉴스가 표시됩니다.")

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
