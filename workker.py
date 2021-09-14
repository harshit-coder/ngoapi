import sys
import time
from datetime import datetime
from telnetlib import EC

from bs4 import BeautifulSoup as soup
from celery import Celery
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from app_moules import logger
from database import db_connect, options

connect = db_connect()

cel = Celery('tasks', backend="rpc://guest:guest@localhost:15672")


@cel.task
def collect_data():
    try:
        driver = webdriver.Chrome('chromedriver', options=options)
        # driver = webdriver.Chrome(executable_path=os.environ.get("chromedriver_PATH"), chrome_options=options)
        driver.get("https://ngodarpan.gov.in/index.php/home/statewise")
        states = driver.find_elements_by_class_name("bluelink11px")
        for state in states:
            try:
                link = state.get_attribute('href')
                state_2 = state.get_attribute("innerHTML")
                sp = soup(state_2, 'html.parser')
                sp = sp.prettify()
                sp = sp.replace("&amp; ", "")
                sp = sp.replace(")", "")
                sp = sp.replace("(", "")
                sp = sp.replace(u'\xa0', u' ')
                sp = sp.strip()
                link = str(link)
                sp_list = sp.split(" ")
                state_name = ""
                for i in range(0, len(sp_list) - 1):
                    state_name = state_name + " " + sp_list[i]
                number = sp_list[len(sp_list) - 1]
                num = int(number)
                pages = int(num / 100) + 1
                row_in_l_p = int(num % 100)
                state_name = state_name.strip()
                link = link.strip()
                sql = 'SELECT * FROM states WHERE state_name = "{}"'.format(str(state_name))
                # sql = 'UPDATE states SET state_name= "{}", total_ngos ="{}", pages = "{}", row_in_l_p = "{}" , url="{}", extract_status="{}", date="{}" WHERE state_name = "{}";'.format(str(state_name), str(num), str(pages), str(row_in_l_p), str(link), "updates success", str(datetime.now()), str(state_name))
                c = connect.query_database_df(sql)
                if c.empty:
                    sql = 'INSERT INTO states(state_name, total_ngos, pages, row_in_l_p, url, extract_status, date) VALUES("{}","{}","{}","{}","{}","{}","{}");'.format(str(state_name), str(num), str(pages), str(row_in_l_p), str(link), "insert success", str(datetime.now()))
                    connect.query_database(sql)
                    line = str(datetime.now()), "DATA INSERTED", str(state_name)
                    logger.info(f"{line}")
                else:
                    line = str(datetime.now()), "DATA EXISTS", str(state_name)
                    logger.info(f"{line}")

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logger.info("exception in collect_data " + str(e) + ' ' + str(exc_tb.tb_lineno))
        driver.close()

        sql = 'SELECT * FROM states'
        c = connect.query_database_df(sql)
        for p in range(0, len(c)):
            url = c['url'][p].strip()
            pages = int(c['pages'][p])
            num = int(c['total_ngos'][p])
            page = pages + 1
            for j in range(1, page):
                if j == pages:
                    row = int(c['row_in_l_p'][p])
                else:
                    row = 100
                state_name = c['state_name'][p].strip()
                page_no = str(j)
                length = len(url)
                url_1 = url.replace(url[length - 1], str(j))
                url_2 = url_1 + "?per_page=100"
                try:
                    driver = webdriver.Chrome('chromedriver', options=options)
                    # driver = webdriver.Chrome(executable_path=os.environ.get("chromedriver_PATH"), chrome_options=options)
                    driver.get(url_2)
                    driver.close()
                    collect.delay(row, url_2, state_name, page_no, pages, num)
                    time.sleep(450)
                    sql = 'DELETE FROM ngo_details WHERE state_name = "{}" AND page_no ="{}" AND extract_status = "{}"'.format(str(state_name), str(page_no), "page insert fail")
                    c = connect.query_database(sql)
                    # add a deletion query check whether the statename page name and url are in table page not extracted if its then delete it
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    logger.info("exception in collect_data " + str(e) + ' ' + str(exc_tb.tb_lineno))
                    print("url" + url_2 + "page" + str(j) + "not extracted")
                    date = datetime.now()
                    state_name = state_name
                    page_no = str(j)
                    url = url_2
                    extract_status = "page not extracted"
                    # chage the query chheck in table page not extracted
                    sql = 'SELECT * FROM ngo_details WHERE url ="{}"'.format(str(url_2))
                    # sql = 'UPDATE ngo_details SET state_name= "{}", total_ngos ="{}", pages = "{}", row_in_l_p = "{}" , url="{}", extract_status="{}", date="{}", page_no ="{}"  WHERE url="{}";'.format(str(state_name), str(num), str(pages), str(row), str(url_2), "page updates fail", str(datetime.now()), str(page_no), str(url_2))
                    c = connect.query_database_df(sql)
                    print("c", c)
                    if c.empty:
                        # chage the query insert in table page not extracted
                        sql = 'INSERT INTO ngo_details(state_name, total_ngos , pages , row_in_l_p  , url, extract_status, date, page_no ) VALUES("{}","{}","{}","{}","{}","{}","{}","{}") ;'.format(str(state_name), str(num), str(pages), str(row), str(url_2), "page insert fail", str(datetime.now()), str(page_no))
                        connect.query_database(sql)
                        line = str(datetime.now()), "PAGE NOT SAVED  State :" + str(state_name) + " Page no: " + str(page_no) + " Url :" + str(url)
                        logger.info(f"{line}")
                    else:
                        line = str(datetime.now()), "PAGE NOT SAVED AGAIN  State :" + str(state_name) + " Page no:" + str(page_no) + " Url:" + str(url)
                        logger.info(f"{line}")
                continue

        print()
        print("------------------Insertion or updating completed--------------------------------------------------------------------------------------------------------------------")
        print()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logger.info("exception in collect_data " + str(e) + ' ' + str(exc_tb.tb_lineno))


@cel.task
def collect(row, url_2, state_name, page_no, pages, num):
    try:
        count = 0
        for i in range(1, int(row) + 1):
            count = count + 1
            driver = webdriver.Chrome('chromedriver', options=options)
            # driver = webdriver.Chrome(executable_path=os.environ.get("chromedriver_PATH"), chrome_options=options)
            driver.get(url_2)
            try:
                print(str(i))
                # driver.implicitly_wait(10)
                all = driver.find_element_by_xpath("/html/body/div[9]/div[1]/div[3]/div/div/div[2]/table/tbody/tr[" + str(i) + "]/td[2]/a")
                all.click()
                time.sleep(10)
                overlay = WebDriverWait(driver, 10).until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "div.blockUI.blockOverlay")))
                if overlay:
                    close = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//*[@id='ngo_info_modal']/div[2]/div/div[1]/button/span")))
                    close.click()
                    time.sleep(2)
                ngo = driver.find_element_by_id('ngo_info_modal')
                result = ngo.get_attribute('innerHTML')
                sp = soup(result, 'html.parser')
                ngo_name = sp.find(id="ngo_name_title")
                ngo_address = sp.find(id="address")
                ngo_date = sp.find(id="ngo_reg_date")
                ngo_mobile = sp.find(id="mobile_n")
                ngo_email = sp.find(id="email_n")
                row_no = str(i)
                date = datetime.now()
                ngo_name = ngo_name.text
                ngo_address = ngo_address.text
                ngo_mobile = ngo_mobile.text
                ngo_date = ngo_date.text
                ngo_email = ngo_email.text
                sql = 'SELECT * FROM ngo_details WHERE ngo_mobile = "{}" '.format(str(ngo_mobile).strip())
                # sql = 'UPDATE ngo_details SET state_name= "{}", total_ngos ="{}", pages = "{}", row_in_l_p = "{}" , url="{}", extract_status="{}", date="{}", page_no ="{}", row_no="{}", ngo_name="{}",ngo_address="{}", ngo_mobile="{}" , ngo_reg_date="{}" , ngo_email="{}" WHERE ngo_mobile="{}";'.format(str(state_name), str(num), str(pages), str(row), str(url_2), "row update success", str(datetime.now()), str(page_no), str(row_no), str(ngo_name), str(ngo_address), str(ngo_mobile), str(ngo_date), str(ngo_email), str(ngo_mobile))
                c = connect.query_database_df(sql)
                if c.empty:
                    sql = 'INSERT INTO ngo_details(state_name, total_ngos , pages , row_in_l_p , url, extract_status, date, page_no , row_no, ngo_name,ngo_address, ngo_mobile, ngo_reg_date , ngo_email) VALUES("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}");'.format(str(state_name).strip(), str(num).strip(), str(pages).strip(), str(row).strip(), str(url_2).strip(), "row insert success", str(datetime.now()).strip(), str(page_no).strip(), str(row_no).strip(), str(ngo_name).strip(), str(ngo_address).strip().replace("\n", " "), str(ngo_mobile).strip(), str(ngo_date).strip(), str(ngo_email).strip())
                    connect.query_database(sql)
                    line = str(date), "ROW  INSERTED... State : " + str(state_name) + " Page no: " + str(page_no) + " Url: " + str(url_2) + " Row no: " + str(i) + " Ngo name: " + str(ngo_name) + " Ngo address: " + str(ngo_address) + " Ngo Registration date: " + str(ngo_date) + " Ngo mobile no: " + str(ngo_mobile) + " Ngo email id: " + str(ngo_email)
                    logger.info(f"{line}")
                else:
                    line = str(date), "ROW  EXISTS... State : " + str(state_name) + " Page no: " + str(page_no) + " Url: " + str(url_2) + " Row no: " + str(i) + " Ngo name: " + str(ngo_name) + " Ngo address: " + str(ngo_address) + " Ngo Registration date: " + str(ngo_date) + " Ngo mobile no: " + str(ngo_mobile) + " Ngo email id: " + str(ngo_email)
                    logger.info(f"{line}")
                sql = 'DELETE FROM ngo_details WHERE state_name = "{}" AND page_no ="{}" AND row_no = "{}" AND extract_status = "{}"'.format(str(state_name), str(page_no), str(row_no), "row insert fail")
                c = connect.query_database(sql)

                driver.close()
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logger.info("exception in collect " + str(e) + ' ' + str(exc_tb.tb_lineno))
                date = datetime.now()
                row_no = str(i)
                # change the query now check in table row_not_extracted
                sql = 'SELECT * FROM ngo_details WHERE url="{}"'.format(str(url_2))
                # sql = 'UPDATE ngo_details SET state_name= "{}", total_ngos ="{}", pages = "{}", row_in_l_p = "{}" , url="{}", extract_status="{}", date="{}", page_no ="{}", row_no="{}"  WHERE  url="{}" ;'.format(str(state_name), str(num), str(pages), str(row), str(url_2), "row update fail", str(datetime.now()), str(page_no), str(row_no), str(url_2))
                c = connect.query_database_df(sql)
                if c.empty:
                    # change the query now insert it into row_not extracted
                    sql = 'INSERT INTO ngo_details(state_name, total_ngos , pages , row_in_l_p  , url, extract_status, date, page_no , row_no) VALUES ("{}","{}","{}","{}","{}","{}","{}","{}","{}")  ;'.format(str(state_name).strip(), str(num).strip(), str(pages).strip(), str(row).strip(), str(url_2).strip(), "row insert fail", str(datetime.now()).strip(), str(page_no).strip(), str(row_no).strip())
                    connect.query_database(sql)
                    line = str(date) + " ROW  NOT SAVED...  State: " + str(state_name) + " Page no: " + str(page_no) + " Url: " + str(url_2) + "Row no:" + str(i)
                    logger.info(f"{line}")

                else:
                    line = str(date) + "ROW  NOT SAVED AGAIN  State: " + str(state_name) + " Page no: " + str(page_no) + " Url: " + str(url_2) + " Row no: " + str(i)
                    logger.info(f"{line}")
            continue
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logger.info("exception in collect " + str(e) + ' ' + str(exc_tb.tb_lineno))


@cel.task
def collect_data_update():
    try:
        driver = webdriver.Chrome('chromedriver', options=options)
        # driver = webdriver.Chrome(executable_path=os.environ.get("chromedriver_PATH"), chrome_options=options)
        driver.get("https://ngodarpan.gov.in/index.php/home/statewise")
        states = driver.find_elements_by_class_name("bluelink11px")
        for state in states:
            try:
                link = state.get_attribute('href')
                state_2 = state.get_attribute("innerHTML")
                sp = soup(state_2, 'html.parser')
                sp = sp.prettify()
                sp = sp.replace("&amp; ", "")
                sp = sp.replace(")", "")
                sp = sp.replace("(", "")
                sp = sp.replace(u'\xa0', u' ')
                sp = sp.strip()
                link = str(link)
                sp_list = sp.split(" ")
                state_name = ""
                for i in range(0, len(sp_list) - 1):
                    state_name = state_name + " " + sp_list[i]
                number = sp_list[len(sp_list) - 1]
                num = int(number)
                pages = int(num / 100) + 1
                row_in_l_p = int(num % 100)
                state_name = state_name.strip()
                link = link.strip()
                # sql = 'SELECT * FROM states WHERE state_name = "{}"'.format(str(state_name))
                sql = 'UPDATE states SET state_name= "{}", total_ngos ="{}", pages = "{}", row_in_l_p = "{}" , url="{}", extract_status="{}", date="{}" WHERE state_name = "{}";'.format(str(state_name), str(num), str(pages), str(row_in_l_p), str(link), "updates success", str(datetime.now()), str(state_name))
                c = connect.query_database(sql)
                if len(c) == 0:
                    sql = 'INSERT INTO states(state_name, total_ngos, pages, row_in_l_p, url, extract_status, date) VALUES("{}","{}","{}","{}","{}","{}","{}");'.format(str(state_name), str(num), str(pages), str(row_in_l_p), str(link), "insert success", str(datetime.now()))
                    connect.query_database(sql)
                    line = str(datetime.now()), "DATA INSERTED", str(state_name)
                    logger.info(f"{line}")
                else:
                    line = str(datetime.now()), "DATA UPDATED", str(state_name)
                    logger.info(f"{line}")

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logger.info("exception in collect_data_update " + str(e) + ' ' + str(exc_tb.tb_lineno))
        driver.close()

        sql = 'SELECT * FROM states'
        c = connect.query_database_df(sql)
        for p in range(0, len(c)):
            url = c['url'][p].strip()
            pages = int(c['pages'][p])
            num = int(c['total_ngos'][p])
            page = pages + 1
            for j in range(1, page):
                if j == pages:
                    row = int(c['row_in_l_p'][p])
                else:
                    row = 100
                state_name = c['state_name'][p].strip()
                page_no = str(j)
                length = len(url)
                url_1 = url.replace(url[length - 1], str(j))
                url_2 = url_1 + "?per_page=100"
                try:
                    driver = webdriver.Chrome('chromedriver', options=options)
                    # driver = webdriver.Chrome(executable_path=os.environ.get("chromedriver_PATH"), chrome_options=options)
                    driver.get(url_2)
                    driver.close()
                    collect_update.delay(row, url_2, state_name, page_no, pages, num)
                    time.sleep(450)
                    sql = 'DELETE FROM ngo_details WHERE state_name = "{}" AND page_no ="{}" AND extract_status = "{}"'.format(str(state_name), str(page_no), "page update fail")
                    c = connect.query_database(sql)
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    logger.info("exception in collect_data_update " + str(e) + ' ' + str(exc_tb.tb_lineno))
                    print("url" + url_2 + "page" + str(j) + "not extracted")
                    date = datetime.now()
                    state_name = state_name
                    page_no = str(j)
                    url = url_2
                    extract_status = "page not extracted"
                    # sql = 'SELECT * FROM ngo_details WHERE url ="{}"'.format(str(url_2))
                    sql = 'UPDATE ngo_details SET state_name= "{}", total_ngos ="{}", pages = "{}", row_in_l_p = "{}" , url="{}", extract_status="{}", date="{}", page_no ="{}"  WHERE url="{}";'.format(str(state_name), str(num), str(pages), str(row), str(url_2), "page update fail", str(datetime.now()), str(page_no), str(url_2))
                    c = connect.query_database(sql)
                    print("c", c)
                    if len(c) == 0:
                        sql = 'INSERT INTO ngo_details(state_name, total_ngos , pages , row_in_l_p  , url, extract_status, date, page_no ) VALUES("{}","{}","{}","{}","{}","{}","{}","{}") ;'.format(str(state_name), str(num), str(pages), str(row), str(url_2), "page insert fail", str(datetime.now()), str(page_no))
                        connect.query_database(sql)
                        line = str(datetime.now()), "PAGE NOT UPDATED  State :" + str(state_name) + " Page no: " + str(page_no) + " Url :" + str(url)
                        logger.info(f"{line}")
                    else:
                        line = str(datetime.now()), "PAGE NOT UPDATED AGAIN  State :" + str(state_name) + " Page no:" + str(page_no) + " Url:" + str(url)
                        logger.info(f"{line}")
                continue

        print()
        print("------------------Insertion or updating completed--------------------------------------------------------------------------------------------------------------------")
        print()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logger.info("exception in collect_data_update " + str(e) + ' ' + str(exc_tb.tb_lineno))


@cel.task
def collect_update(row, url_2, state_name, page_no, pages, num):
    try:
        count = 0
        for i in range(1, int(row) + 1):
            count = count + 1
            driver = webdriver.Chrome('chromedriver', options=options)
            # driver = webdriver.Chrome(executable_path=os.environ.get("chromedriver_PATH"), chrome_options=options)
            driver.get(url_2)
            try:
                print(str(i))
                # driver.implicitly_wait(10)
                all = driver.find_element_by_xpath("/html/body/div[9]/div[1]/div[3]/div/div/div[2]/table/tbody/tr[" + str(i) + "]/td[2]/a")
                all.click()
                time.sleep(10)
                overlay = WebDriverWait(driver, 10).until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "div.blockUI.blockOverlay")))
                if overlay:
                    close = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//*[@id='ngo_info_modal']/div[2]/div/div[1]/button/span")))
                    close.click()
                    time.sleep(2)
                ngo = driver.find_element_by_id('ngo_info_modal')
                result = ngo.get_attribute('innerHTML')
                sp = soup(result, 'html.parser')
                ngo_name = sp.find(id="ngo_name_title")
                ngo_address = sp.find(id="address")
                ngo_date = sp.find(id="ngo_reg_date")
                ngo_mobile = sp.find(id="mobile_n")
                ngo_email = sp.find(id="email_n")
                row_no = str(i)
                date = datetime.now()
                ngo_name = ngo_name.text
                ngo_address = ngo_address.text
                ngo_mobile = ngo_mobile.text
                ngo_date = ngo_date.text
                ngo_email = ngo_email.text
                # sql = 'SELECT * FROM ngo_details WHERE ngo_mobile = "{}" '.format(str(ngo_mobile).strip())
                sql = 'UPDATE ngo_details SET state_name= "{}", total_ngos ="{}", pages = "{}", row_in_l_p = "{}" , url="{}", extract_status="{}", date="{}", page_no ="{}", row_no="{}", ngo_name="{}",ngo_address="{}", ngo_mobile="{}" , ngo_reg_date="{}" , ngo_email="{}" WHERE ngo_mobile="{}";'.format(str(state_name), str(num), str(pages), str(row), str(url_2), "row update success", str(datetime.now()), str(page_no), str(row_no), str(ngo_name), str(ngo_address), str(ngo_mobile), str(ngo_date), str(ngo_email), str(ngo_mobile))
                c = connect.query_database(sql)
                if len(c) == 0:
                    sql = 'INSERT INTO ngo_details(state_name, total_ngos , pages , row_in_l_p , url, extract_status, date, page_no , row_no, ngo_name,ngo_address, ngo_mobile, ngo_reg_date , ngo_email) VALUES("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}");'.format(str(state_name).strip(), str(num).strip(), str(pages).strip(), str(row).strip(), str(url_2).strip(), "row insert success", str(datetime.now()).strip(), str(page_no).strip(), str(row_no).strip(), str(ngo_name).strip(), str(ngo_address).strip().replace("\n", " "), str(ngo_mobile).strip(), str(ngo_date).strip(), str(ngo_email).strip())
                    connect.query_database(sql)
                    line = str(date), "ROW  INSERTED... State : " + str(state_name) + " Page no: " + str(page_no) + " Url: " + str(url_2) + " Row no: " + str(i) + " Ngo name: " + str(ngo_name) + " Ngo address: " + str(ngo_address) + " Ngo Registration date: " + str(ngo_date) + " Ngo mobile no: " + str(ngo_mobile) + " Ngo email id: " + str(ngo_email)
                    logger.info(f"{line}")

                else:
                    line = str(date), "ROW  UPDATED... State : " + str(state_name) + " Page no: " + str(page_no) + " Url: " + str(url_2) + " Row no: " + str(i) + " Ngo name: " + str(ngo_name) + " Ngo address: " + str(ngo_address) + " Ngo Registration date: " + str(ngo_date) + " Ngo mobile no: " + str(ngo_mobile) + " Ngo email id: " + str(ngo_email)
                    logger.info(f"{line}")
                sql = 'DELETE FROM ngo_details WHERE state_name = "{}" AND page_no ="{}" AND row_no = "{}" AND extract_status = "{}"'.format(str(state_name), str(page_no), str(row_no), "row update fail")
                c = connect.query_database(sql)

                driver.close()
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logger.info("exception in collect_update " + str(e) + ' ' + str(exc_tb.tb_lineno))
                date = datetime.now()
                row_no = str(i)
                # sql = 'SELECT * FROM ngo_details WHERE url="{}"'.format(str(url_2))
                sql = 'UPDATE ngo_details SET state_name= "{}", total_ngos ="{}", pages = "{}", row_in_l_p = "{}" , url="{}", extract_status="{}", date="{}", page_no ="{}", row_no="{}"  WHERE  url="{}" ;'.format(str(state_name), str(num), str(pages), str(row), str(url_2), "row update fail", str(datetime.now()), str(page_no), str(row_no), str(url_2))
                c = connect.query_database(sql)
                if len(c) == 0:
                    sql = 'INSERT INTO ngo_details(state_name, total_ngos , pages , row_in_l_p  , url, extract_status, date, page_no , row_no) VALUES ("{}","{}","{}","{}","{}","{}","{}","{}","{}")  ;'.format(str(state_name).strip(), str(num).strip(), str(pages).strip(), str(row).strip(), str(url_2).strip(), "row insert fail", str(datetime.now()).strip(), str(page_no).strip(), str(row_no).strip())
                    connect.query_database(sql)
                    line = str(date) + " ROW  NOT UPDATED... State: " + str(state_name) + " Page no: " + str(page_no) + " Url: " + str(url_2) + "Row no: " + str(i)
                    logger.info(f"{line}")
                else:
                    line = str(date) + "ROW  NOT UPDATED AGAIN  State: " + str(state_name) + " Page no: " + str(page_no) + " Url: " + str(url_2) + " Row no: " + str(i)
                    logger.info(f"{line}")
            continue
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logger.info("exception in collect_update " + str(e) + ' ' + str(exc_tb.tb_lineno))




if __name__ == '__main__':
    cel.start()
