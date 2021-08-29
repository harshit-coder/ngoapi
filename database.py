import MySQLdb
from sqlalchemy import create_engine

import pandas as pd
class db_connect:
    def __init__(self):
        self.db_read = MySQLdb.connect(host="127.0.0.1",
                                       user="harshit",
                                       passwd="22441011",
                                       db="ngo")
        self.db_read.autocommit(True)
        self.sql_engine = create_engine("mysql+mysqldb://harshit:22441011@127.0.0.1/ngo")

    def close_connection(self):
        try:
            self.db_read.close()
        except Exception as e:
            print(str(e))

    def query_database(self, sql, **kwargs):
        conn = self.db_read
        cursor = conn.cursor()
        try:
            rows = cursor.execute(sql)
            cursor.fetchall()
            print("rows",rows)
            return rows
        except Exception as e:
            print(str(e))
            if "Lost connection to MySQL server during query" in str(e) or "MySQL server has gone away" in str(e) or "(0, '')" in str(e) or "(2013" in str(e):
                try:
                    try:
                        self.close_connection()
                    except Exception:
                        pass
                    self.__init__()
                    conn = self.db_read
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    no_of_rows = cursor.rowcount
                    return rows
                except Exception as e:
                    print(str(e))
            return [], 0
