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

def insert_genres(conn, cur, df):
    insert_genre_query = "INSERT IGNORE INTO Genre (Movie_id, Genre_name) VALUES (%s, %s)"
    
    for index, row in df.iterrows():
        movie_id = row['영화id']
        genres = row['장르'].split(',') if pd.notna(row['장르']) else []
        
        if not genres:
            continue
        
        try:
            genre_data = [(movie_id, genre.strip()) for genre in genres]
            cur.executemany(insert_genre_query, genre_data)
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

# 데이터베이스에 장르 정보를 삽입
def insert_genres_to_db(df):
    conn, cur = open_db()
    insert_genres(conn, cur, df)
    close_db(conn, cur)

# 영화 장르 정보를 데이터베이스에 삽입
insert_genres_to_db(df)
