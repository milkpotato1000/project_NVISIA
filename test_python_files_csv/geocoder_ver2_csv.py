import folium
import random
import json
import psycopg2
from psycopg2.extras import RealDictCursor

class Geocoder:

    def __init__(self, host, database, user, password, port):
        """
        
        [작성 2025-12-02]
        postgis 호출하여 geometry 값을 지도에 표시.
        최종적으로 streamlit 통해 dashboard에 구현.

        input: 기사 id(사용자가 클릭한 기사)
        1) click_id 기사 및 관련 추천 기사 10개의 id를 rec.py 를 통해 가져옴
        2) 해당 기사들의 event_loc 조회
        3) geojson table에서 geometry 호출
        4) folium 지도에 시각화

        [수정 2025-12-04]
        issue: 지도에 여러개의 기사가 아닌 클릭한 기사 하나만 표시.
        solve: get_map 외에 get_map_single 함수 추가.

        [추가 2025-12-15] ver2
        issue: postgres에 북한 주요 위치 좌표 테이블 추가. 해당 테이블로부터 데이터 불러온 뒤, 공간 join 필요.
        solve: 함수 추가. 

        """

        # Postgre db 연결
        self.conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)

    def get_event_loc(self, ids):
        """
        
        [작성 2025-12-03]
        click 기사와 추천 10개 기사들의 event_loc을 전부 호출하여 list로 저장. 
        list를 get_map 에 전달하여 지도에 출력.

        """

        query = """
            SELECT event_loc
            FROM summary
            WHERE id = ANY(%s)
            """
        self.cur.execute(query, (ids,))
        rows = self.cur.fetchall()

        event_locs = []
        for row in rows:
            locs = row["event_loc"]
            if not locs:
                continue
            for loc in locs.split(","):
                loc = loc.strip()
                if loc:
                    event_locs.append(loc)
            
        event_locs = list(dict.fromkeys(event_locs))
        return event_locs
    
    def get_geometry(self, event_locs):
        """
        
        [작성 2025-12-03]
        get_event_loc 에서 반환된 event_locs 에 매칭되는 geometry 호출.
        ST_AsGeoJSON(geometry) 쿼리를 통해 PostGIS geometry 를 json 으로 변환.

        """

        if not event_locs:
            return {}
        
        query = """
            SELECT event_loc, ST_AsGeoJSON(geometry) AS geojson
            FROM geojson
            WHERE event_loc = ANY(%s)
        """
        self.cur.execute(query, (event_locs,))
        rows = self.cur.fetchall()

        result = {}
        for row in rows:
            geo = json.loads(row["geojson"])
            result[row["event_loc"]] = geo
        return result
                         
    def get_random_color(self):
        return "#%06x" % random.randint(0, 0xFFFFFF)
    
    def get_map(self, click_id, recommender, k=10, location=(39.0, 127.0), zoom_start=7):
        """

        [작성 2025-12-03]
        get_geometry 로 추출한 results를 지도에 표시.
        추천 시스템은 rec.py 로부터 호출.
                
        """

        rec_list = recommender.get_similar_articles(click_id, k)
        rec_ids = [item["id"] for item in rec_list]
        target_ids = [click_id] + rec_ids

        # 클릭 기사와 추천 기사들의 event_loc 추출
        event_locs = self.get_event_loc(target_ids)

        # 클릭 기사와 추천 기사들의 geometry 추출
        geo_dict = self.get_geometry(event_locs)

        # Folium 맵 객체 초기화
        m = folium.Map(location=location, zoom_start=zoom_start)

        # 지도에 표시
        for loc, geojson_data in geo_dict.items():            
            style_function = lambda x, color = self.get_random_color(): {
                    'fillColor': color,
                    'color': 'black',
                    'weight': 1,
                    'fillOpacity': 0.7
            }

            # folium.GeoJson 객체 생성 및 지도에 추가
            folium.GeoJson(
                geojson_data,
                name=loc,
                tooltip=folium.Tooltip(loc),
                style_function=style_function
            ).add_to(m)

        return m
    
    def get_map_single(self, click_id, location=(39.0, 127.0), zoom_start=7):
        """
        
        [작성 2025-12-04]
        사용자가 클릭한 단일 기사 하나의 event_loc만 지도에 표시.

        """

        ids = [click_id]
        event_locs = self.get_event_loc(ids)

        # event_loc 없는 경우 빈 지도 반환
        m = folium.Map(location=location, zoom_start=zoom_start)
        if not event_locs:
            return m
        
        geo_dict = self.get_geometry(event_locs)

        # 지도에 표시
        for loc, geojson_data in geo_dict.items():            
            style_function = lambda x, color = self.get_random_color(): {
                    'fillColor': color,
                    'color': 'black',
                    'weight': 1,
                    'fillOpacity': 0.7
            }

            folium.GeoJson(
                geojson_data,
                name=loc,
                tooltip=folium.Tooltip(loc),
                style_function=style_function
            ).add_to(m)

        return m
    
    def get_event_org(self):
        """
        
        [추가 2025-12-15]
        postgres nk_org table로부터 데이터 로드. (epsg:5179 이므로 변환 필요)

        """

        query = """
            SELECT
                org_name AS org_name,
                ST_X(ST_Transform(geom_5179, 4326)) AS x_4326,
                ST_Y(ST_Transform(geom_5179, 4326)) AS y_4326
            FROM nk_org
        """
        self.cur.execute(query)
        return self.cur.fetchall()    
  
    def get_org(self, org_names):
        """

        [추가 2025-12-15]
        selected_id 에 해당하는 event_org 와 동일한 nk_org.org_name 의 좌표를 반환

        """
        if not org_names:
            return []
        
        query = """
        SELECT
            org_name,
            ST_Y(ST_Transform(geom_5179, 4326)) AS y_4326,
            ST_X(ST_Transform(geom_5179, 4326)) AS x_4326
        FROM nk_org
        WHERE org_name = ANY(%s)
        """
        self.cur.execute(query, (org_names,))
        return self.cur.fetchall()
    
    def do_spatial_join(self, event_locs, event_orgs):
        """
        
        [추가 2025-12-15]
        유저가 클릭한 기사의 event_loc에 해당하는 폴리곤 내부에 위치한 북한 주요 위치 좌표만 반환.
        event_loc의 geomtery는 epsg:4326 이므로 5179 변환 후 공간 조인
        
        """
        if not event_locs or not event_orgs:
            return []
        
        query = """
            SELECT
                o.org_name AS org_name,
                g.event_loc,
                ST_Y(ST_Transform(o.geom_5179, 4326)) AS y_4326,
                ST_X(ST_Transform(o.geom_5179, 4326)) AS x_4326
            FROM geojson g
            JOIN nk_org o ON ST_Intersects(ST_Transform(g.geometry, 5179), o.geom_5179)
            WHERE g.event_loc = ANY(%s) AND o.org_name = ANY(%s)
            """
        self.cur.execute(query, (event_locs,event_orgs))
        return self.cur.fetchall()

    def close(self):
        self.cur.close()
        self.conn.close()