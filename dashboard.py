# Streamlit Dashboard to view summary_df_final.csv
# Modified to include dynamic recommendation system based on similarity

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import ast

# Set Korean font for Matplotlib (Windows)
plt.rc('font', family='Malgun Gothic')
plt.rc('axes', unicode_minus=False)

st.set_page_config(page_title="News Data Dashboard", layout="wide")

st.title("NVISIA: North-Korea Vision & Insights by SIA")

# Load CSV (handle possible encoding issues)
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
    """Prepare embeddings for similarity calculation"""
    if 'embedding' not in df.columns:
        st.error("No 'embedding' column found in the data!")
        return None, None
    
    # Convert string representation of list to actual array
    df_copy = df.copy()
    df_copy["embedding"] = df_copy["embedding"].apply(ast.literal_eval)
    embeddings = np.array(df_copy["embedding"].to_list())
    
    # Create id to index mapping for fast lookup
    id_to_index = {id_val: idx for idx, id_val in enumerate(df_copy["id"])}
    
    return embeddings, id_to_index

def get_similar_articles(df, embeddings, id_to_index, click_id, k=10):
    """
    Get k most similar articles based on cosine similarity
    
    Args:
        df: DataFrame with article data
        embeddings: numpy array of embeddings
        id_to_index: dictionary mapping id to index
        click_id: ID of the clicked article
        k: number of recommendations to return
    
    Returns:
        DataFrame with recommended articles
    """
    if click_id not in id_to_index:
        st.warning(f"Article ID {click_id} not found in embeddings!")
        return pd.DataFrame()
    
    click_idx = id_to_index[click_id]
    click_vec = embeddings[click_idx]
    
    # Calculate cosine similarity
    sims = embeddings @ click_vec
    
    # Exclude the clicked article itself
    sims[click_idx] = -1.0
    
    # Get top k indices
    top_idx = np.argsort(-sims)[:k]
    
    # Return recommended articles
    return df.iloc[top_idx]

# Load data
df = load_data(DATA_PATH)

if df.empty:
    st.warning("No data to display.")
else:
    # Prepare embeddings for recommendation system
    embeddings, id_to_index = prepare_embeddings(df)
    
    # Create two columns for the top section with 1:2 ratio
    col1, col2 = st.columns([1, 2])

    # Placeholders for dynamic content
    with col1:
        chart_container = st.empty()
    
    with col2:
        rec_container = st.empty()

    st.divider()

    # Initialize expand state
    if "expanded" not in st.session_state:
        st.session_state.expanded = False
    # Button to toggle expansion
    def toggle_expanded():
        st.session_state.expanded = not st.session_state.expanded

    st.button(
        "Expand table" if not st.session_state.expanded else "Collapse table",
        on_click=toggle_expanded
    )
    # Determine height
    table_height = 600 if st.session_state.expanded else 250
    
    # Create layout: left for dataframe, right for map
    df_col, map_col = st.columns([2, 1])
    
    with df_col:
        # Show full dataframe in a scrollable container with the calculated height
        # Enable row selection
        # Exclude embedding column from display and specify column order
        display_columns = ['id', 'title', 'summary', 'publish_date', 'category']
        # Only include columns that exist in the dataframe
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
            map_image = Image.open("data/map.png") #지도파일 로딩 경로
            st.image(map_image, caption="지리 정보 (향후 PostgreSQL 연동 예정)", width=int(map_image.width * 0.7))
        except FileNotFoundError:
            st.info("지도 이미지를 찾을 수 없습니다. (data/map.png)")
        except Exception as e:
            st.error(f"지도 이미지 로드 오류: {e}")


    # Determine which data to use for the chart and update col2
    if len(event.selection.rows) > 0 and embeddings is not None:
        # Row selected: Get recommendations based on similarity
        selected_idx = event.selection.rows[0]
        selected_id = df.iloc[selected_idx]["id"]
        
        # Get recommended articles
        rec_df = get_similar_articles(df, embeddings, id_to_index, selected_id, k=10)
        
        chart_df = rec_df
        chart_title = "추천 뉴스 카테고리"
        
        # Show Recommended Data Table in col2
        with rec_container.container():
            st.subheader(f"관련 추천 뉴스 (기준: {df.iloc[selected_idx]['id']})")
            if not rec_df.empty:
                # Create a copy for display to truncate long text
                display_df = rec_df[['id', 'title', 'summary', 'category', 'event_date']].copy()
                if 'summary' in display_df.columns:
                    display_df['summary'] = display_df['summary'].apply(lambda x: x[:50] + '...' if isinstance(x, str) and len(x) > 50 else x)
                
                # Use st.dataframe for sortable columns
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    height=300
                )
            else:
                st.info("No recommended data available.")
    else:
        # No row selected: Use Full Data for Chart
        chart_df = df
        chart_title = "전체 뉴스 카테고리"
        
        # Show info message in col2
        rec_container.info("아래 목록에서 기사를 선택하면 추천 뉴스가 표시됩니다.")

    # Draw Pie Chart in col1
    with chart_container.container():
        if 'category' in chart_df.columns:
            st.subheader(chart_title)
            category_counts = chart_df['category'].value_counts()
            
            if not category_counts.empty:
                def autopct_filter(pct):
                    return ('%1.1f%%' % pct) if pct > 5 else ''
                    
                # Small figsize
                fig, ax = plt.subplots(figsize=(1.7, 1.7)) 
                # Labels outside, no rotation
                wedges, texts, autotexts = ax.pie(
                    category_counts, 
                    labels=category_counts.index, 
                    autopct=autopct_filter, 
                    startangle=90, 
                    textprops={'fontsize': 4}
                )
                
                # Small font size for percentages inside the pie
                for autotext in autotexts:
                    autotext.set_fontsize(4)
                    
                ax.axis('equal')
                st.pyplot(fig, use_container_width=False)
            else:
                st.info("No category data to display.")
