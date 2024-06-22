import pandas as pd
import pymysql

def open_db(dbname='moviedb'):
    conn = pymysql.connect(host='localhost',
                           user='uhyeok1',
                           passwd='0000',
                           db=dbname)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    return conn, cur

def close_db(conn, cur):
    cur.close()
    conn.close()

def insert_movie_directors(conn, cur, df):
    select_director_id_query = "SELECT Director_id FROM Director WHERE Director_name = %s"
    insert_movie_director_query = "INSERT IGNORE INTO Movie_Director (Movie_id, Director_id) VALUES (%s, %s)"
    
    movie_director_data = []

    for index, row in df.iterrows():
        if pd.isna(row['감독']):
            continue  # 결측치인 경우 건너뜀
        
        movie_id = row['영화id']
        directors = row['감독'].split(',')
        
        for director in directors:
            director_name = director.strip()
            
            # Director_id 조회
            cur.execute(select_director_id_query, (director_name,))
            director_result = cur.fetchone()
            
            if director_result:
                director_id = director_result['Director_id']
                movie_director_data.append((movie_id, director_id))
    
    if movie_director_data:
        try:
            cur.executemany(insert_movie_director_query, movie_director_data)
            conn.commit()
        except pymysql.MySQLError as e:
            print(f"Error: {e}")
            conn.rollback()

# 엑셀 파일 읽기
file_path = 'movie_list.xlsx'

# 첫 번째 시트 읽기, 앞의 4개 행은 스킵하고 5번째 행을 컬럼 이름으로 사용
sheet1 = pd.read_excel(file_path, sheet_name='영화정보 리스트', header=4)

# 두 번째 시트 읽기, 헤더 없이 데이터만 읽기
sheet2 = pd.read_excel(file_path, sheet_name='영화정보 리스트_2', header=None)

# sheet2에 sheet1의 컬럼 이름을 적용
sheet2.columns = sheet1.columns

# 두 개의 데이터프레임 합치기
df = pd.concat([sheet1, sheet2], ignore_index=True)

# 상위 100개 행만 가져오기
df_top_100 = df.head(100)

# 데이터베이스에 감독 정보를 삽입
def insert_movie_directors_to_db(df):
    conn, cur = open_db()
    try:
        insert_movie_directors(conn, cur, df)
    finally:
        close_db(conn, cur)

# Movie_Director 테이블에 데이터를 삽입
insert_movie_directors_to_db(df)
