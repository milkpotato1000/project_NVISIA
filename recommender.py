import pandas as pd
import numpy as np
import os
import random

# Global variables to hold loaded data
df = None
id_to_index = None

def load_data(data_dir="data"):
    """
    Load data from CSV files.
    """
    global df, id_to_index

    # Try to load full_df_final.csv
    path = os.path.join(data_dir, "full_df_final.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found.")
    
    # Load data
    df = pd.read_csv(path)
    
    # Create id_to_index map
    if 'id' in df.columns:
        id_to_index = {str(row['id']): idx for idx, row in df.iterrows()}
    else:
        raise ValueError("Column 'id' not found in dataframe.")

    print(f"Data loaded: {len(df)} records.")

def get_similar_articles(click_id, k=10):
    """
    Recommend k similar articles for a given click_id.
    NOTE: Since embeddings are missing in the workspace, this uses RANDOM selection for demonstration.
    """
    global df, id_to_index
    
    click_id = str(click_id)

    if df is None:
        raise ValueError("Data not loaded. Call load_data() first.")

    if click_id not in id_to_index:
        return []

    # Mock recommendation: Randomly select k other articles
    all_ids = list(id_to_index.keys())
    if click_id in all_ids:
        all_ids.remove(click_id)
    
    # If we have fewer than k items, return all
    if len(all_ids) < k:
        k = len(all_ids)
        
    recommended_ids = random.sample(all_ids, k)
    
    results = []
    for rid in recommended_ids:
        idx = id_to_index[rid]
        row = df.iloc[idx]
        results.append({
            "id": row['id'],
            "category": row.get('category', ''),
            "publish_date": str(row.get('publish_date', ''))[:10],
            "similarity": round(random.uniform(0.5, 0.9), 4), # Mock similarity
            "keywords": row.get('keywords', ''),
            "event_person": row.get('event_person', '')
        })
    return results

def get_article_details(article_id):
    global df, id_to_index
    
    if df is None:
        return None
        
    if article_id not in id_to_index:
        return None
        
    idx = id_to_index[article_id]
    row = df.iloc[idx]
    
    return {
        "id": row['id'],
        "category": row.get('category', ''),
        "publish_date": str(row.get('publish_date', ''))[:10],
        "keywords": row.get('keywords', ''),
        "event_person": row.get('event_person', ''),
        "summary": row.get('summary', ''),
        "event_title": row.get('event_title', '')
    }
