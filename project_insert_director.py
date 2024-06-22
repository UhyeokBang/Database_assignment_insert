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

def insert_directors(conn, cur, df):
    select_director_query = "SELECT Director_id FROM Director WHERE Director_name = %s"
    insert_director_query = "INSERT IGNORE INTO Director (Director_name) VALUES (%s)"
    
    director_set = set()  # 감독 이름 중복 체크용
    new_directors = []  # 배치 삽입용 리스트

    for index, row in df.iterrows():
        if pd.isna(row['감독']):
            continue  # 결측치인 경우 건너뜀
        
        directors = row['감독'].split(',')
        
        for director in directors:
            director_name = director.strip()
            if director_name in director_set:
                continue  # 이미 삽입된 감독이면 건너뜀
            
            try:
                # 감독이 이미 테이블에 존재하는지 확인
                cur.execute(select_director_query, (director_name,))
                result = cur.fetchone()
                
                if result:
                    director_set.add(director_name)
                    continue  # 이미 존재하면 건너뜀
                
                # 새 감독 리스트에 추가
                new_directors.append((director_name,))
                director_set.add(director_name)
            except pymysql.MySQLError as e:
                print(f"Error: {e}")
                conn.rollback()
    
    if new_directors:
        try:
            cur.executemany(insert_director_query, new_directors)
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
df_top_50 = df.head(50)

# 데이터베이스에 감독 정보를 삽입
def insert_directors_to_db(df):
    conn, cur = open_db()
    try:
        insert_directors(conn, cur, df)
    finally:
        close_db(conn, cur)

# 감독 정보를 데이터베이스에 삽입
insert_directors_to_db(df)
