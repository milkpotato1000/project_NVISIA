# Streamlit Dashboard to view summary_df_final.csv

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

# Set Korean font for Matplotlib (Windows)
plt.rc('font', family='Malgun Gothic')
plt.rc('axes', unicode_minus=False)

st.set_page_config(page_title="News Data", layout="wide")

st.title("News Data")

# Load CSV (handle possible encoding issues)
# Load CSV (handle possible encoding issues)
DATA_PATH = "data/full_df_final.csv"

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

df = load_data(DATA_PATH)

if df.empty:
    st.warning("No data to display.")
else:
    # Category Pie Chart
    if 'category' in df.columns:
        st.subheader("Category Distribution")
        category_counts = df['category'].value_counts()
        def autopct_filter(pct):
            return ('%1.1f%%' % pct) if pct > 5 else ''
            
        fig, ax = plt.subplots(figsize=(1, 1))
        wedges, texts, autotexts = ax.pie(category_counts, labels=category_counts.index, autopct=autopct_filter, startangle=90, textprops={'fontsize': 4}, rotatelabels=True)
        
        # Increase font size for percentages inside the pie
        for autotext in autotexts:
            autotext.set_fontsize(6)
            
        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        st.pyplot(fig, use_container_width=False)

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
    table_height = 500 if st.session_state.expanded else 250
    # Show full dataframe in a scrollable container with the calculated height
    st.dataframe(df, height=table_height, use_container_width=True)
    st.caption(f"Showing {len(df)} rows – scroll to view the rest.")
