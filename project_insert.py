import pandas as pd
import pymysql
from pymysql.constants.CLIENT import MULTI_STATEMENTS

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

def insert_movies(conn, cur, df):
    insert_query = """
    INSERT INTO Movie (Movie_name, Movie_name_eng, Production_year, Production_country, Type, Production_status, Production_company)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    data = [
        (
            row['영화명'],
            row['영화명(영문)'] if pd.notna(row['영화명(영문)']) else None,
            row['제작연도'] if pd.notna(row['제작연도']) else None,
            row['제작국가'] if pd.notna(row['제작국가']) else None,
            row['유형'] if pd.notna(row['유형']) else None,
            row['제작상태'] if pd.notna(row['제작상태']) else None,
            row['제작사'] if pd.notna(row['제작사']) else None
        )
        for index, row in df.iterrows()
    ]

    try:
        cur.executemany(insert_query, data)
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

# 상위 10개 행만 가져오기
df_top_100 = df.head(100)

# 데이터베이스에 영화 정보를 삽입
def insert_movies_to_db(df):
    conn, cur = open_db()

    insert_movies(conn, cur, df)
        
    close_db(conn, cur)

# 영화 정보를 데이터베이스에 삽입
insert_movies_to_db(df)
