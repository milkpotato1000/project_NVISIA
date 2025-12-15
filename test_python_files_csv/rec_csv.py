import psycopg2
from psycopg2.extras import RealDictCursor

class Recommender:

    def __init__(self, host, database, user, password, port):
        """

        [작성 2025-12-01]
        LLMtoDatabase class로 저장된 summary 파일 호출하여 기사를 추천.
        
        input: 사용자가 선택한 기사 id (click_id)
        output: 관련 기사 

        실행 방법은 rec.ipynb 파일 참고.

        [수정 2025-12-05]
        issue: knowledge graph 표현 위해서는 Recommender의 출력값에 keywords도 필요
        solve: 출력 result에 keywords 추가

        """

        # Postgre db 연결
        self.conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)

    def get_similar_articles(self, click_id, k):
        """

        [작성 2025-12-01]
        click_id: 사용자가 클릭한 기사 id
        k: 추천할 기사의 개수

        click_id의 embedding값을 참고하여 코사인 유사도를 계산.
        (pgvector의 코사인 유사도 계산은 정규화 과정을 자동으로 진행)

        반환: json 형식.
        
        """

        query = """
            WITH base AS(
                SELECT embedding
                FROM summary
                WHERE id = %s
            )
            SELECT
                s.id,
                s.keywords,
                s.category,
                s.publish_date,
                s.title,
                s.url,
                1 - (s.embedding <=> b.embedding) AS similarity
            FROM summary s
            CROSS JOIN base b
            WHERE s.id <> %s
            ORDER BY s.embedding <=> b.embedding
            LIMIT %s;
        """

        self.cur.execute(query, (click_id, click_id, k))
        rows = self.cur.fetchall()

        results = []
        for row in rows:
            results.append(
                {
                    "id": row["id"],
                    "keywords": row["keywords"],
                    "category": row["category"],
                    "publish_date": str(row["publish_date"])[:10],
                    "title": row["title"],
                    "url": row["url"]
                }
            )

        return results

    def close(self):
        self.cur.close()
        self.conn.close()