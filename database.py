import logging
import os

import pymysql

from selenium import webdriver
from sqlalchemy import create_engine

import pandas as pd

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
options = webdriver.ChromeOptions()
options.add_argument(f'user-agent={user_agent}')
#options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
options.headless = True
options.add_argument("--window-size=1920,1080")
options.add_argument('--ignore-certificate-errors')
options.add_argument('--allow-running-insecure-content')
options.add_argument("--disable-extensions")
options.add_argument("--proxy-server='direct://'")
options.add_argument("--proxy-bypass-list=*")
options.add_argument("--start-maximized")
options.add_argument('--disable-gpu')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--no-sandbox')
driver = webdriver.Chrome('/chromedriver', options=options)
#driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=options)



# create logger with log app
real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)
LOGFILE = f"{dir_path}/test.log"
logger = logging.getLogger('log_app')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(LOGFILE)
formatter = logging.Formatter('%(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# for g in f:
# i = str("DATE :" + f['date'][g]+ " STATE : " + f['state_name'][g]+ " PAGE NO : " + f['page_no'][g]+  " URL : " +  f['url'][g] + " ROW_NO : " +  f['row_no'][g] + " NGO NAME : " +  f['ngo_name'][g] + " NGO ADDRESS : " +  f['ngo_address'][g] + "NGO MOBILE : " +  f['state_name'][g])


real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)
LOGFILE = f"{dir_path}/test.log"


class db_connect:
    def __init__(self):
        self.db_read = pymysql.connect(host="ngodatatapi.c9jzn7xnv0df.ap-south-1.rds.amazonaws.com",
                                       user="root",
                                       passwd="22441011",
                                       db="ngo")
        self.db_read.autocommit(True)
        self.sql_engine = create_engine("mysql+pymysql://root:22441011@ngodatatapi.c9jzn7xnv0df.ap-south-1.rds.amazonaws.com/ngo")

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
                    rows = cursor.execute(sql)
                    cursor.fetchall()
                    no_of_rows = cursor.rowcount
                    return rows
                except Exception as e:
                    print(str(e))
            return [], 0

    def query_database_df(self, sql, **kwargs):
        conn = self.db_read
        try:
            df = pd.read_sql(sql, con=conn)
            return df
        except Exception as e:
              if "Lost connection to MySQL server during query" in str(e) or \
                    "MySQL server has gone away" in str(e) or "(0, '')" in str(e):
                try:
                    try:
                        self.close_connection()
                    except Exception as e_:
                        pass
                    self.__init__()
                    df = pd.read_sql(sql, con=self.db_read)
                    return df
                except Exception as e:
                    print("Error in retrying connecting to mysql server " + str(e), 'error')
              return None
