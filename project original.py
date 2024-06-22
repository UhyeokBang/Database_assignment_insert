import pandas as pd
import numpy as np
from db_conn import *

import sys


def read_excel_into_mysql():
    excel_file = "movie_list.xls"

    conn, cur = open_db()

    df = pd.read_excel(excel_file, skiprows=4)

    print(df.head())

    
    movie_table = "university.movie"

    create_sql = f"""
        drop table if exists {movie_table} ;

        create table {movie_table} (
            id int auto_increment primary key,
            title varchar(500),
            eng_title varchar(500),
            year int,
            country varchar(100),
            m_type varchar(10),
            genre varchar(100),
            status varchar(30),
            director varchar(100),
            company varchar(100),
            enter_date datetime default now()
        ); """

    cur.execute(create_sql)
    conn.commit()


    insert_sql = f"""insert into {movie_table} (title, eng_title, year, country, m_type, genre, status, director, company)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

    for i, r in df.iterrows():
        row = tuple(r)

        try:
            cur.execute(insert_sql, row)
            if (i+1) % 1000 == 0:
                print(f"{i} rows")
        except Exception as e:
            pass
            print(e)
            print(row)

    conn.commit()
    
    close_db(conn, cur)
       


if __name__ == '__main__':
    read_excel_into_mysql()    
