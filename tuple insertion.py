import pymysql
import random
import string

from db_conn import *
    
    
def create_10M_student_table():
    conn, cur = open_db()
    insert_sql = """
        insert into student10M(sname, dept, year, memo) values(%s, %s, %s, %s);
        """
        
    rows = []
    i = 0
    
    while i <= 10000000:
        sname = ''.join(random.choices(string.ascii_uppercase, k=4))
        dept = random.randint(1,100)
        year = random.randint(1,4)
        memo = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=500))
        r = (sname, dept, year, memo)
        rows.append(r)
        i += 1
        
        if i % 10000 == 0:
            cur.executemany(insert_sql, rows)
            conn.commit()
            print("%d rows" %i)
            rows = []
            
    close_db(conn, cur)
    
    
def create_1M_course_table():
    conn, cur = open_db()
    insert_sql = """
        insert into course1M(cname, dept, memo) values(%s,%s,%s);
        """
    
    rows = []
    i = 0
    
    while i <= 1000000:
        cname = ''.join(random.choices(string.ascii_uppercase, k=4))
        dept = random.randint(1,100)
        memo = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=500))
        r = (cname, dept, memo)
        rows.append(r)
        i += 1
        
        if i % 10000 == 0:
            cur.executemany(insert_sql, rows)
            conn.commit()
            print("%d rows" %i)
            rows = []
            
    close_db(conn, cur)
    

def create_1M_enrol_table():
    try:
        conn, cur = open_db()
        insert_sql = """
            insert into enrol1M(sno, cno, midterm, final, memo) values(%s, %s, %s, %s, %s);
        """
        
        rows = []
        i = 0
        
        while i <= 1000000:
            sno = random.randint(1, 10000000)
            cno = random.randint(1, 1000000)
            midterm = random.randint(1, 100)
            final = random.randint(1, 100)
            memo = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=500))
            r = (sno, cno, midterm, final, memo)
            rows.append(r)
            i += 1
            
            if i % 10000 == 0:
                try:
                    cur.executemany(insert_sql, rows)
                    conn.commit()
                    print("%d rows on enrol" % i)
                except pymysql.MySQLError as e:
                    print(f"Error on batch {i}: {e}")
                rows = []
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        close_db(conn, cur)
    

if __name__ == '__main__':
    #create_10M_student_table()    
    #create_1M_course_table()
    create_1M_enrol_table()            
