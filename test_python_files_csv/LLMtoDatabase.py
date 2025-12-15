import os
import pandas as pd
import numpy as np
import re
import io

# llm
from dotenv import load_dotenv
from openai import OpenAI
import json

# db
import psycopg2

# tfidf
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle

# .env 파일 로드
load_dotenv()  # 현재 디렉토리 내 .env 파일 정보를 환경변수로 읽어옴

# 환경변수 확인
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OPENAI_API_KEY not found. .env 또는 시스템 환경변수에 설정하세요.")

client = OpenAI(api_key=api_key)
llm_model = "gpt-4o-mini"
embed_model = "text-embedding-ada-002"

class LLMtoDatabase:

    def __init__(self, host, database, user, password, port, tfidf_vectorizer_path, svm_model_path, label_encoder_path):
        """

        CSV(title, contents, publish_date, url) 파일을 받아 LLM 요약.
        LLM ouput Postgre DB table에 저장.

        [수정 2025-11-27]
        1) CSV(title, contents, publish_date, url) 파일을 input으로 받음.
        2) LLM summary, keywords 등등 출력.
        3) summary, keywords TF-IDF 변환 > SVM > 카테고리 분류.
        4) ADA embedding 진행 > db 저장. (postgres에서 l2 norm 진행)
        5) Postgre DB table에 저장.

        [수정 2025-12-03]
        매개변수 tfidf_vectorizer_path, svm_model_path, label_encoder_path 추가.
        '시', '도' 변환 코드 merge.

        [수정 2025-12-15]
        issue: csv reader가 없음
        solve: csv 인코딩 추가

        """

        # 모델 정의
        self.llm_model = llm_model
        self.embed_model = embed_model

        # Postgre db 연결
        self.conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        self.cur = self.conn.cursor()

        # TF-IDF pickle 파일 로드
        with open(tfidf_vectorizer_path, "rb") as f:
            self.tfidf_vectorizer = pickle.load(f)

        with open(svm_model_path, "rb") as f:
            self.svm_model = pickle.load(f)

        with open(label_encoder_path, "rb") as f:
            self.label_encoder = pickle.load(f) 
        
        # Load nk_cities and build maps for normalization
        try:
            self.nk_cities = pd.read_csv('data/nk_cities.csv', encoding='euc-kr')
            self.provinces_map, self.cities_map = self._build_maps()
            self.BROAD_TERMS_MAP = {
                "평안도": ["평안남도", "평안북도"],
                "함경도": ["함경남도", "함경북도"],
                "황해도": ["황해남도", "황해북도"]
            }
        except Exception as e:
            print(f"Warning: Failed to load nk_cities.csv or build maps. Normalization will be skipped. Error: {e}")
            self.nk_cities = None
            self.provinces_map = {}
            self.cities_map = {}
            self.BROAD_TERMS_MAP = {}

    @staticmethod
    def read_csv(file_bytes):
        """

        [추가 2025-12-15]
        Streamlit file uploader를 통해 받은 bytes를 csv로 로드

        """
        last_err = None
        for enc in ("utf-8-sig", "cp949", "euc-kr", "utf-8"):
            try:
                return pd.read_csv(io.BytesIO(file_bytes), encoding=enc)
            except Exception as e:
                last_err = e
        raise last_err
    
    def _get_search_keys(self, name):
        if pd.isna(name): return [], None
        # Handle parentheses: "나선시(라선시)" -> parts: ["나선시", "라선시"]
        parts = re.split(r'[()]', name)
        parts = [p.strip() for p in parts if p.strip()]
        
        canonical_name = parts[0] # The first part is the canonical name
        
        keys = []
        for p in parts:
            # Strip suffixes '도', '시', '군', '구역' for search key
            key = p
            if key.endswith('도'): key = key[:-1]
            elif key.endswith('시'): key = key[:-1]
            elif key.endswith('군'): key = key[:-1]
            elif key.endswith('구역'): key = key[:-1]
            keys.append(key)
        return keys, canonical_name

    def _build_maps(self):
        provinces_map = {} # search_key -> canonical_full_name
        cities_map = {}    # search_key -> {'full': canonical_full_name, 'province': province_canonical_name}

        for idx, row in self.nk_cities.iterrows():
            # Process Province
            p_keys, p_canon = self._get_search_keys(row['도'])
            for k in p_keys:
                provinces_map[k] = p_canon
                
            # Process City
            c_keys, c_canon = self._get_search_keys(row['시'])
            for k in c_keys:
                cities_map[k] = {
                    'full': c_canon,
                    'province': p_canon # This might be None or a string
                }

        # Manual additions for abbreviations and broader terms
        abbr_map = {
            '평남': '평안남도',
            '평북': '평안북도',
            '함남': '함경남도',
            '함북': '함경북도',
            '황남': '황해남도',
            '황북': '황해북도',
            '양강': '양강도',
            '자강': '자강도',
            '강원': '강원도',
            '평안도': '평안도', # Broader term
            '황해도': '황해도', # Broader term
            '함경도': '함경도',  # Broader term
            '평안': '평안도' # Example 7: "평안" -> "평안도" (Assuming broader term)
        }

        for abbr, full in abbr_map.items():
            provinces_map[abbr] = full
            
        return provinces_map, cities_map

    def map_location_normalized(self, loc_str):
        if pd.isna(loc_str) or not isinstance(loc_str, str):
            return None
        
        found_provinces = set()
        found_cities = [] # List of dicts
        
        # 1. Search for Provinces
        for key, full_name in self.provinces_map.items():
            if key in loc_str:
                found_provinces.add(full_name)
                
        # 2. Search for Cities
        for key, info in self.cities_map.items():
            if key in loc_str:
                match_info = info.copy()
                match_info['key'] = key
                found_cities.append(match_info)
                
        # 3. Consolidate and Remove Redundancy
        
        # 3a. Identify implied provinces from found cities
        implied_provinces = set()
        for c in found_cities:
            if pd.notna(c['province']):
                implied_provinces.add(c['province'])
                
        # 3b. Remove found provinces if they are implied by the cities
        temp_provinces = set()
        for p in found_provinces:
            if p not in implied_provinces:
                temp_provinces.add(p)
        
        # 3c. Remove Broad Terms if Specific Terms are present
        all_present_specific_provinces = temp_provinces.union(implied_provinces)
        
        final_provinces = set()
        for p in temp_provinces:
            is_redundant_broad = False
            if p in self.BROAD_TERMS_MAP:
                # Check if any specific term for this broad term is present
                for specific in self.BROAD_TERMS_MAP[p]:
                    if specific in all_present_specific_provinces:
                        is_redundant_broad = True
                        break
            
            if not is_redundant_broad:
                final_provinces.add(p)
                
        # 4. Format Output
        final_results = set()
        
        # Add Remaining Provinces
        for p in final_provinces:
            final_results.add(p)
            
        # Add Cities (Format: "Province City" or "City")
        for c in found_cities:
            full_city = c['full']
            province = c['province']
            
            if pd.notna(province):
                final_results.add(f"{province} {full_city}")
            else:
                final_results.add(full_city)
                
        if not final_results:
            return None
            
        return ', '.join(sorted(list(final_results)))

    
    def get_article_summary(self, title, contents, publish_date):
        """
        뉴스 기사를 LLM으로 요약하고 항목별 데이터 반환
        
        Change Log:
            @@ 11.18 
            > 프롬프트 수정
                - 키값 정리 형식 재 설정 - 데이터 프레임의 컬럼명이 키값이 되도록 수정
                - 추가적인 컬럼 event_person 추가
                - event_obj 컬럼명 event_org로 변경
            @@ 11.19
            > 프롬프트 수정
                - 인물명은 이름만 명확하게
                - 지역명은 [국가, 도, 시] 단위로 명확하게
                - 평양 쌀, 옥수수, 달러환율 정보는 각기 별개의 데이터로 저장
                - 키워드는 인물명, 지역명을 반드시 포함 + 요약내용 대표 단어 추가
            > result 파싱 방식 수정
                - eval()에서 jason.load()로 변경
        """
        
        prompt = f"""
    아래 기사를 분석하여 요구된 정보를 작성하시오.

    # 기사 제목:
    {title}

    # 기사 내용:
    {contents}

    # 기사 작성일:
    {publish_date}

   1. 아래 형식으로 정리 (괄호안 각 key값의 한글 설명은 참고만 하고 최종 결과에는 포함하지 않음)
    - summary(주요 사건 요약):
    - event_title(사건 주제):
    - event_date(사건 발생일):
    - event_person(사건 핵심 인물):
    - event_org(사건 핵심 조직/기관):
    - event_loc(사건 발생 지명):
    - keywords(주요 키워드):
    
    2. 각 카테고리의 조건
    - "summary": 3 문장 이하로 핵심 내용만 발췌.
    - "event_title": 간단한 한 문장으로 사건 주제 작성.
    - "event_date": yyyy-mm-dd 형식, 기사에 "event_date"가 명시되지 않았으면 "기사 내용" 중 시간 또는 기간을 나타내는 단어(예시로, '어제', '사흘전', '일주일 전' 등)를 참고하여 "기사 작성일" 기준 계산.
    - "event_person": 사건의 주체 인물(들)의 이름만 입력, 다수의 경우 쉼표로 구분.
    - "event_org": 사건의 주체 조직 및 기관의 이름만 입력, 다수의 경우 쉼표로 구분, **언론사명은 반드시 제외**, **신문사명은 반드시 제외**, **기자가 참고한 출처의 이름도 반드시 제외**, **"노동신문"은 반드시 제외**.
    - "event_loc": [도, 시]단위 지명만을 입력하되 "도" 와 "시" 정보가 함께 있는 경우는 반드시 행적구역별로 분리해서 입력. 건물등에서 일어난 사건의 경우는 해당 장소의 [도, 시] 지명을 입력, 행정구역이 "시"일 경우는 꼭 "시"를 명시 (개성시, 평양시, 고성시 등). 
    특히 "평양" / "평양직할시" / "평양시"와 같이 한 지명에 다양한 표기가 있을경우는 "평양시" ([시 이름] + 시)와 같은 형태로 통일. **"북한" 이라는 단어는 반드시 제외**. 북한이 아닌 해외의 사건의 경우만 국가명을 입력.
    - "keywords": "summary", "event_title", "event_person", "event_org", "event_loc" 모두를 종합적으로 고려하여 해당 뉴스 사건을 대표할 수 있는 **단어 5개 선정**, **"북한" 이라는 단어는 반드시 제외**, 쉼표로 구분하여 입력.
    
    - 위 결과를 종합하여 딕셔너리 형태로 출력.
    - 결과를 출력하기 전 다음 체크리스트를 스스로 검증하라:
        - [ ] 내가 사용한 모든 답과 수치는 기사 원문에 존재한다.
        
    - 설명 출력 금지, 답만 출력.
    """

        try:
            response = client.chat.completions.create(
                model=self.llm_model,
                messages=[
                    {"role": "system", "content": "당신은 북한 관련 뉴스 사건 정보를 추출하는 전문 분석 모델입니다."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0
            )
            
            # 문자열을 dict로 변환
            result_text = response.choices[0].message.content.strip()
            try:
                result = json.loads(result_text)  # json 파싱으로 수정(11.19)
            except:
                print("Parsing error:", result_text)
                return None
            
            # [NEW] Normalize event_loc
            if 'event_loc' in result:
                normalized_loc = self.map_location_normalized(result['event_loc'])
                if normalized_loc:
                    result['event_loc'] = normalized_loc
                  
            return result
            
        except Exception as e:
            print("Error in LLM call or parsing:", e)
            return None

    def value_to_strCSV(self, value):
        """
        리스트 또는 쉼표 문자열을 'a, b, c' 형태의 문자열로 표준화
        """
        if not value:
            return ""

        # 이미 리스트이면 → 요소 strip 후 join
        if isinstance(value, list):
            return ", ".join(x.strip() for x in value)

        # 문자열이면 split → 다시 join 처리
        if isinstance(value, str):
            return ", ".join(x.strip() for x in value.split(","))

        # 그 외 타입은 문자열로 강제 변환
        return str(value)
    
    def insert_summary(self, llm, title, publish_date, url, category, embedding):
        """
        
        LLM output 과 원본 csv 파일의 title, publish_date, url 데이터를 postgre table에 저장

        [수정 2025-11-24]
        issue: 동일 csv 파일로 코드 재실행 시, 이미 db에 등록된 contents가 새로운 id로 재등록.
        수정: 동일한 url이 재입력 시, pass 
        상세: query >> ON CONFLICT (url) DO NOTHING << 추가

        [수정 2025-12-03]
        categorizer, embedding model update
        llm_to_db.ipynb 코드 병합.

        """

        query = """
            INSERT INTO summary
                (summary, keywords, event_title, event_date,
                 event_person, event_org, event_loc, url, title, publish_date, category, embedding)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING;
        """

        values = (
            llm.get("summary"),
            self.value_to_strCSV(llm.get("keywords")),
            llm.get("event_title"),
            llm.get("event_date"),
            self.value_to_strCSV(llm.get("event_person")),
            self.value_to_strCSV(llm.get("event_org")),
            self.value_to_strCSV(llm.get("event_loc")),
            url,
            title, 
            publish_date,
            category,
            embedding.tolist(),
        )

        try:
            self.cur.execute(query, values)
            self.conn.commit()

            if self.cur.rowcount == 0:
                print(f"[DB INSERT ERROR] 이미 존재하는 기사입니다. url={url}")

        except Exception as e:
            self.conn.rollback()
            print(f"[DB INSERT ERROR] url={url} ⇒ {e}")

    def check_url(self, url):
        """

        [추가 2025-11-24]
        issue: LLM 처리 후 url 중복 체크 시, LLM 토큰 낭비.
        해결: LLM 처리 이전 url 사전 체크 함수 추가. 
        상세: 
        코드 재실행 시 동일한 기사가 새로운 ID를 부여받지 않도록 방지.
        url을 조회하여 있는 경우 skip.

        """
        query = """
            SELECT COUNT(*) FROM summary
            WHERE url = %s;
        """

        self.cur.execute(query, (url,))
        count = self.cur.fetchone()[0]

        return count > 0

    def preprocess_text(self, text):
        """

        [추가 2025-11-27]
        issue: SVM 모델 구동 위해 형태 동일하게 변환.
        해결: s_news_categorizer 전처리 코드와 동일한 로직.

        """
        if pd.isna(text): 
            return ""
        text = str(text).lower() 
        text = re.sub(r'[^가-힣a-zA-Z0-9\s]', '', text) 
        return text    

    def get_category(self, summary, keywords):
        """

        [추가 2025-11-27]
        issue: 모델 분류기가 tf-idf를 활용함에 따라 tf-idf 변환 필요.
        해결: s_news_categorizer 전처리 코드 merge.
        상세: s_news_categorizer.ipynb 동일. 
        
        [수정 2025-11-28]
        summary + keywords 를 input을 받는 vectorizer 로 변경.
        vectorizer >> svm >> label encoding

        """
        preprocessed_summary = self.preprocess_text(summary)
        preprocessed_keywords = self.preprocess_text(keywords)
        combined_text = preprocessed_summary + " " + preprocessed_keywords

        X_combined = self.tfidf_vectorizer.transform([combined_text])

        svm_pred = self.svm_model.predict(X_combined)[0]

        category = self.label_encoder.inverse_transform([svm_pred])[0]
        return category

    def text_to_embedding(self, text):
        """

        [추가 2025-11-27]
        issue: 모델 분류기는 tf-idf를 활용하지만, 추천시스템은 embedding 활용.
        해결: embedding 코드 추가.
        상세: summary + keywords 를 임베딩 후 수평합. 해당 값은 postgres에 저장.

        [수정 2025-11-28]
        issue: text_embedding 라이브러리와 파이썬 버전 충돌
        해결: text_embeddings[0].embedding >> text_embeddings.data[0].embedding

        """
        text = text
        text_embeddings = client.embeddings.create(
            model = self.embed_model,
            input = text
        )
        embeddings = np.array(text_embeddings.data[0].embedding, dtype = np.float32)
        return embeddings

    def get_embeddings(self, summary, keywords):
        """

        [추가 2025-11-27]
        issue: 모델 분류기는 tf-idf를 활용하지만, 추천시스템은 embedding 활용.
        해결: embedding 코드 추가.
        상세: text_to_embedding 함수 받아와 summary, keywords 임베딩.

        """
        embed_summary = self.text_to_embedding(summary)
        embed_keywords = self.text_to_embedding(keywords)
        embed_rec = np.hstack([embed_summary, embed_keywords])
        return embed_rec
    
    def csv_to_db(self, file_bytes):
        """
        
        [추가 2025-12-15]
        입력 받은 bytes를 db에 넣기 위해 실행하는 함수.
        
        """
        df = self.read_csv(file_bytes)

        required_cols = {"title", "content", "publish_date", "url"}
        missing = required_cols - set(df.columns)        
        if missing:
            raise ValueError(f"CSV에 필수 컬럼이 없습니다: {missing}")       
        
        stats = {
            "total": len(df),
            "inserted": 0,
            "skipped_existing": 0,
            "skipped_empty": 0,
            "failed": 0,
        }

        for i, row in df.iterrows():
            try:
                title = row.get("title", "") or ""
                content = row.get("content", "") or ""
                publish_date = row.get("publish_date", "") or ""
                url = row["url"]

                if not isinstance(content, str) or not content.strip():
                    stats["skipped_empty"] += 1
                    continue

                if self.check_url(url):
                    stats["skipped_existing"] += 1
                    continue

                llm = self.get_article_summary(title, content, publish_date)
                if not llm:
                    stats["failed"] += 1
                    continue

                summary_text = llm.get("summary", "") or ""
                keywords_text = self.value_to_strCSV(llm.get("keywords"))

                category = self.get_category(summary_text, keywords_text)
                embedding = self.get_embeddings(summary_text, keywords_text)

                self.insert_summary(
                    llm=llm,
                    title=title,
                    publish_date=publish_date,
                    url=url,
                    category=category,
                    embedding=embedding,
                )

                stats["inserted"] += 1     

            except Exception as e:
                stats["failed"] += 1
                print(f"[CSV INGEST ERROR] row={i}, url={row.get('url')}: {e}")

        return stats

    def close(self):
        self.cur.close()
        self.conn.close()