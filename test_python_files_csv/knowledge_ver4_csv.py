import pandas as pd
import networkx as nx
from sklearn.feature_extraction.text import TfidfVectorizer
from community import community_louvain
import collections
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import networkx as nx
import os
import matplotlib.font_manager as fm
import textwrap

class KnowledgeGraph:

    def __init__(self, rec_list):
        """
        
        [작성 2025-12-10]
        rec.py 의 list output을 받아서 knowledge graph를 출력.

        input: rec_list(id, keywords, category, publish_date, title, url - json 양식)
        output: graph

        [수정 2025-12-11]
        issue: 1개의 기사와 1개의 키워드만 연결된 엣지들이 너무 많아서 그래프가 번잡함.
        solve: 최소 2개의 기사 노드와 연결된 키워드 노드만 표시하도록 코드 수정.

        [수정 2025-12-15] (ver4)
        issue: Graph Edge Color를 Keyword Node Color와 일치시키기.
        solve: get_figure 메서드에서 edge 그릴 때, 연결된 keyword 노드의 community color를 참조하여 edge_color 리스트 생성.

        [수정 2025-12-16]
        issue: kg 생성 시 list index out of range 에러 발생.
        solve: 기존 id가 'spnews_100' 형태여서 split("_")[1] 구조로 코드를 작성했었음 > [-1]로 수정  

        """

        self.dataframe = pd.DataFrame(rec_list)
        if self.dataframe.empty:
            raise ValueError("추천 기사가 없거나, 키워드가 누락되었습니다.")    
        self.dataframe['processed_keywords'] = self.dataframe['keywords'].apply(self.preprocess_korean_text)
        self.get_tfidf()
        self.article_relationships = self.get_relationships()
        self.G = self.set_graph()
        self.apply_tfidf()
        self.partition, self.num_communities, self.community_sizes = self.apply_louvain()

    def preprocess_korean_text(self, text):
        if not isinstance(text, str):
            return ''

        all_keywords = []
        for part in text.split(','):
            all_keywords.extend(part.strip().split())

        filtered_keywords = [word for word in all_keywords if word]

        return ' '.join(filtered_keywords)
   
    def get_relationships(self):
        article_relationships = []

        for i in range(len(self.dataframe)):
            for j in range(i + 1, len(self.dataframe)):
                article1 = self.dataframe.iloc[i]
                article2 = self.dataframe.iloc[j]

                keywords1 = set(article1['processed_keywords'].split())
                keywords2 = set(article2['processed_keywords'].split())

                common_keywords = list(keywords1.intersection(keywords2))

                if common_keywords:
                    article_relationships.append((article1['id'], article2['id'], common_keywords))
                
        return article_relationships

    def set_graph(self):
        # 1. 비어있는 그래프 G를 생성합니다.
        G = nx.Graph()

        # 2. 각 기사에 대해 'article' 타입의 노드를 그래프에 추가합니다.
        for _, row in self.dataframe.iterrows():
            G.add_node(row['id'], type='article', title=row['title'])

        # 3. dataframe['processed_keywords'] 컬럼에서 모든 고유한 키워드를 추출하여 'keyword' 타입의 노드를 그래프에 추가합니다.
        all_keywords = set()
        for keywords_str in self.dataframe['processed_keywords']:
            all_keywords.update(keywords_str.split())

        for keyword in all_keywords:
            G.add_node(keyword, type='keyword')

        # 4. 각 기사와 해당 기사의 'processed_keywords'에 포함된 키워드 사이에 엣지를 추가합니다.
        for _, row in self.dataframe.iterrows():
            article_id = row['id']
            article_keywords = row['processed_keywords'].split()
            for keyword in article_keywords:
                if keyword:
                    G.add_edge(article_id, keyword, type='ARTICLE_KEYWORD')

        # 5. article_relationships 리스트를 사용하여 공통 키워드를 공유하는 기사들 사이에 엣지를 추가합니다.
        for id1, id2, common_keywords in self.article_relationships:
            if common_keywords:
                G.add_edge(id1, id2, type='SHARED_KEYWORD', common_keywords=common_keywords)
        
        return G

    def get_tfidf(self):
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(self.dataframe['processed_keywords'])

        self.feature_names = vectorizer.get_feature_names_out()

        self.tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=self.feature_names, index=self.dataframe['id'])

    def apply_tfidf(self):

        # 4. Iterate through all unique keywords and update keyword nodes in G
        for keyword in self.feature_names:
            # Calculate global importance score for each keyword (e.g., max TF-IDF across all articles)
            # Check if the keyword exists in tfidf_df columns before trying to access it
            if keyword in self.tfidf_df.columns:
                global_tfidf_score = self.tfidf_df[keyword].max()
            else:
                global_tfidf_score = 0.0 # Should not happen if feature_names are from vectorizer of processed_keywords

            # Update the corresponding keyword node in the graph G
            # Ensure the node exists and is of type 'keyword'
            if self.G.has_node(keyword) and self.G.nodes[keyword]['type'] == 'keyword':
                self.G.nodes[keyword]['tfidf_score'] = global_tfidf_score        

        # 5. Iterate through all existing edges in the graph G and assign weights
        for u, v, data in self.G.edges(data=True):
            edge_type = data.get('type')

            if edge_type == 'ARTICLE_KEYWORD':
                article_id = u if self.G.nodes[u]['type'] == 'article' else v
                keyword = v if self.G.nodes[v]['type'] == 'keyword' else u

                # Retrieve TF-IDF score for this specific keyword in this article
                if article_id in self.tfidf_df.index and keyword in self.tfidf_df.columns:
                    tfidf_score = self.tfidf_df.loc[article_id, keyword]
                    self.G[u][v]['weight'] = tfidf_score
                else:
                    self.G[u][v]['weight'] = 0.0 # Should not happen if data is consistent

            elif edge_type == 'SHARED_KEYWORD':
                article1_id = u
                article2_id = v
                common_keywords = data.get('common_keywords', [])
                total_weight = 0.0

                for kw in common_keywords:
                    # Add TF-IDF of the keyword in article1
                    if article1_id in self.tfidf_df.index and kw in self.tfidf_df.columns:
                        total_weight += self.tfidf_df.loc[article1_id, kw]
                    # Add TF-IDF of the keyword in article2
                    if article2_id in self.tfidf_df.index and kw in self.tfidf_df.columns:
                        total_weight += self.tfidf_df.loc[article2_id, kw]
                self.G[u][v]['weight'] = total_weight        

    def apply_louvain(self):
        # 2. Run Louvain community detection algorithm
        partition = community_louvain.best_partition(self.G)

        # 3. Add community information to each node as a 'community' attribute
        for node, comm_id in partition.items():
            self.G.nodes[node]['community'] = comm_id        

        # 4. Print the number of detected communities and node counts per community
        num_communities = len(set(partition.values()))
        community_sizes = collections.Counter(partition.values())

        return partition, num_communities, community_sizes
    
    @staticmethod
    def setup_korean_font():
        """
        한글 폰트 셋업을 안 할 경우, UserWarning: Glyph 50896 발생.
        DejaVu Sans 에서 한글 폰트를 지원하지 않아서 전부 깨져서 출력됨.
        """
        available_fonts = {f.name for f in fm.fontManager.ttflist}

        preferred_fonts = [
            "Malgun Gothic",   # 맑은 고딕
            "NanumGothic",     # 나눔고딕 (영문 이름)
            "Nanum Gothic",    # 나눔고딕 (띄어쓰기 버전)
        ]

        for name in preferred_fonts:
            if name in available_fonts:
                plt.rcParams["font.family"] = name
                plt.rcParams["axes.unicode_minus"] = False
                print(f"[Font] Using Korean font: {name}")
                return
            
    @staticmethod
    def text_split(text, max_width):
        """
        노드 라벨이 너무 길면 자동 줄바꿈.
        - 공백 있는 경우: textwrap 으로 단어 단위 줄바꿈
        - 공백 없는 한글 단어: 글자 수 기준으로 잘라서 줄바꿈
        """
        if not isinstance(text, str):
            return text

        text = text.strip()
        if len(text) <= max_width:
            return text

        # 공백 있는 경우
        if " " in text:
            return "\n".join(textwrap.wrap(text, width=max_width))

        return "\n".join(
            text[i : i + max_width] for i in range(0, len(text), max_width)
        )

    def get_graph(self):
        self.setup_korean_font()
        # 감지된 고유 커뮤니티 수를 가져옵니다.
        self.num_communities = len(set(self.partition.values()))

        # 커뮤니티를 위한 색상 맵을 생성합니다.
        # matplotlib의 컬러맵을 사용하여 각 커뮤니티에 대해 다른 색상을 얻습니다.
        colors = list(mcolors.TABLEAU_COLORS.values()) + list(mcolors.CSS4_COLORS.values())
        community_colors = {comm_id: colors[i % len(colors)] for i, comm_id in enumerate(sorted(set(self.partition.values())))}

        # 노드를 유형별로 분리하고 해당 속성 리스트를 생성합니다.
        article_nodes = []
        keyword_nodes = []
        article_labels = {}
        keyword_labels = {}
        article_colors = []
        keyword_colors = []

        # ordered_nodelist는 G.nodes()의 순서를 유지합니다.
        ordered_nodelist = list(self.G.nodes())

        for node_id in ordered_nodelist:
            attributes = self.G.nodes[node_id]
            comm_id = attributes.get('community', -1)
            node_color = community_colors.get(comm_id, 'gray')

            if attributes['type'] == 'article':
                article_nodes.append(node_id)
                article_labels[node_id] = attributes['title']
                article_colors.append('lightsteelblue') # Modified: Unified color for article nodes
            elif attributes['type'] == 'keyword':
                keyword_nodes.append(node_id)
                keyword_labels[node_id] = node_id
                keyword_colors.append(node_color)

        plt.figure(figsize=(20, 15)) # 그래프 크기 조정
        pos = nx.spring_layout(self.G, k=1.5, iterations=50, dim=2, seed=42) # k: 노드 간 거리, iterations: 레이아웃 계산 반복 횟수. 재현성을 위해 seed 추가

        if not pos:
            print("오류: nx.spring_layout이 빈 위치 사전을 반환했습니다. 이는 그래프가 비어 있거나 문제가 있음을 의미할 수 있습니다.")
        else:
            # 엣지를 먼저 그려 노드 뒤에 위치하도록 합니다.
            nx.draw_networkx_edges(self.G, pos, edge_color='gray', width=0.5)

            # 기사 노드 (사각형) 그리기
            if article_nodes:
                nx.draw_networkx_nodes(self.G, pos,
                                    nodelist=article_nodes,
                                    node_shape='s',
                                    node_color=article_colors,
                                    node_size=3000)
                nx.draw_networkx_labels(self.G, pos,
                                        labels=article_labels,
                                        font_size=24,
                                        font_color='black',
                                        font_weight='bold',
                                        font_family=plt.rcParams["font.family"])
                
            # 키워드 노드 (원형) 그리기
            if keyword_nodes:
                nx.draw_networkx_nodes(self.G, pos,
                                    nodelist=keyword_nodes,
                                    node_shape='o',
                                    node_color=keyword_colors,
                                    node_size=10000)
                nx.draw_networkx_labels(self.G, pos,
                                        labels=keyword_labels,
                                        font_size=24,
                                        font_color='black',
                                        font_family=plt.rcParams["font.family"])
                
            # plt.title('Knowledge Graph with Louvain Communities', size=20) # 제목 추가
            plt.axis('off') # 축 제거
            plt.show()

    def get_figure(self):
        """

        [작성 2025-12-10]
        streamlit 구현 위한 figure 리턴 함수 추가.
        위의 get_graph 의 출력값을 plt >> figure로 수정한 버전.

        """
        self.setup_korean_font()

        # 감지된 고유 커뮤니티 수를 가져옵니다.
        self.num_communities = len(set(self.partition.values()))

        colors = list(mcolors.TABLEAU_COLORS.values())[5:] 
        community_colors = {comm_id: colors[i % len(colors)] for i, comm_id in enumerate(sorted(set(self.partition.values())))}

        # 노드를 유형별로 분리하고 해당 속성 리스트를 생성합니다.
        article_nodes = []
        keyword_nodes = []
        article_labels = {}
        keyword_labels = {}
        article_colors = []
        keyword_colors = []

        # ordered_nodelist는 G.nodes()의 순서를 유지합니다.
        ordered_nodelist = list(self.G.nodes())

        for node_id in ordered_nodelist:
            attributes = self.G.nodes[node_id]
            comm_id = attributes.get('community', -1)
            node_color = community_colors.get(comm_id, '#999999')

            if attributes['type'] == 'article':
                article_nodes.append(node_id)
                article_labels[node_id] = str(node_id).split("_")[-1]
                article_colors.append("#000000") 

            elif attributes['type'] == 'keyword':

                # 최소 2개의 기사 노드와 연결된 키워드 노드만 표시
                article_neighbor_count = sum(
                    1
                    for i in self.G.neighbors(node_id)
                    if self.G.nodes[i].get("type") == "article"
                )
                if article_neighbor_count < 2:
                    continue

                keyword_nodes.append(node_id)
                keyword_labels[node_id] = node_id
                keyword_colors.append(node_color)

        wrapped_keyword_labels = {
            node_id: self.text_split(label, max_width=4)  
            for node_id, label in keyword_labels.items()
        }

        fig, ax = plt.subplots(figsize=(12, 8)) # 그래프 크기 조정
        pos = nx.spring_layout(self.G, k=1.5, iterations=50, dim=2, seed=42) # k: 노드 간 거리, iterations: 레이아웃 계산 반복 횟수. 재현성을 위해 seed 추가

        if not pos:
            print("오류: nx.spring_layout이 빈 위치 사전을 반환했습니다. 이는 그래프가 비어 있거나 문제가 있음을 의미할 수 있습니다.")
        else:        
            # 기사-키워드만 연결
            keyword_nodes_set = set(keyword_nodes)

            ak_edges = []
            edge_colors = []

            for u, v, d in self.G.edges(data=True):
                if d.get("type") == "ARTICLE_KEYWORD" and (u in keyword_nodes_set or v in keyword_nodes_set):
                    ak_edges.append((u, v))
                    
                    # 엣지 색상 결정: 연결된 노드 중 keyword 타입인 노드의 community 색상 따라감
                    target_color = "black" # fallback
                    
                    if u in keyword_nodes_set: # u가 키워드 노드라면
                        comm_id = self.G.nodes[u].get('community', -1)
                        target_color = community_colors.get(comm_id, '#999999')
                    elif v in keyword_nodes_set: # v가 키워드 노드라면
                        comm_id = self.G.nodes[v].get('community', -1)
                        target_color = community_colors.get(comm_id, '#999999')
                    
                    edge_colors.append(target_color)

            nx.draw_networkx_edges(
                self.G,
                pos,
                edgelist=ak_edges,
                edge_color=edge_colors, # 수정된 부분: 리스트로 색상 전달
                width=1.5, # 조금 더 잘 보이게 두께 조정
                alpha=0.7,
                ax=ax,
            )

        # 기사 노드 (사각형) 그리기
        if article_nodes:
            nx.draw_networkx_nodes(self.G, pos,
                                nodelist=article_nodes,
                                node_shape='s',
                                node_color=article_colors,
                                node_size=3000,
                                ax=ax)
            nx.draw_networkx_labels(self.G, pos,
                                    labels=article_labels,
                                    font_size=17,
                                    font_color='yellow',
                                    font_weight='bold',
                                    font_family=plt.rcParams["font.family"],
                                    ax=ax)
        
        # 키워드 노드 (원형) 그리기
        if keyword_nodes:
            nx.draw_networkx_nodes(self.G, pos,
                                nodelist=keyword_nodes,
                                node_shape='o',
                                node_color=keyword_colors,
                                node_size=5000,
                                ax=ax)
            nx.draw_networkx_labels(self.G, pos,
                                    labels=wrapped_keyword_labels,
                                    font_size=15,
                                    font_color='black',
                                    font_family=plt.rcParams["font.family"],
                                    ax=ax)

        ax.axis("off")
        fig.tight_layout()

        return fig
