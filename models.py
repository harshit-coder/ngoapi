import time
from datetime import datetime

from bs4 import BeautifulSoup as soup
from celery import Celery
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from database import db_connect

cel = Celery('tasks', broker="amqp://guest:guest@localhost")
connect = db_connect()


@cel.task
def collect_state():
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
    options = webdriver.ChromeOptions()
    options.add_argument(f'user-agent={user_agent}')

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

    count = 0
    driver = webdriver.Chrome('./chromedriver', options=options)
    driver.get("https://ngodarpan.gov.in/index.php/home/statewise")
    states = driver.find_elements_by_class_name("bluelink11px")
    d1 = {}
    for state in states:
        try:
            count = count + 1
            link = state.get_attribute('href')
            state_2 = state.get_attribute("innerHTML")
            # print(state_2)
            sp = soup(state_2, 'html.parser')
            sp = sp.prettify()
            sp = sp.replace("&amp; ", "")
            sp = sp.replace(")", "")
            sp = sp.replace("(", "")
            sp = sp.replace(u'\xa0', u' ')
            sp = sp.strip()
            print(sp)
            print(link)
            link = str(link)
            sp_list = sp.split(" ")
            print(sp_list)
            state_name = ""
            for i in range(0, len(sp_list) - 1):
                state_name = state_name + " " + sp_list[i]
            number = sp_list[len(sp_list) - 1]
            num = int(number)
            pages = int(num / 100) + 1
            row_in_l_p = int(num % 100)
            state_name = state_name.strip()
            link = link.strip()
            sql = 'UPDATE states SET state_name= "{}", total_ngos ="{}", pages = "{}", row_in_l_p = "{}" , url="{}", extract_status="{}", date="{}" WHERE state_name = "{}";' .format(str(state_name), str(num), str(pages), str(row_in_l_p), str(link), "updates success", str(datetime.now()), str(state_name))
            c = connect.query_database(sql)
            print("c",c)
            if c == 0:
                sql = 'INSERT INTO states(state_name, total_ngos, pages, row_in_l_p, url, extract_status, date) VALUES("{}","{}","{}","{}","{}","{}","{}");' .format(str(state_name), str(num), str(pages), str(row_in_l_p), str(link), "insert success", str(datetime.now()))
                connect.query_database(sql)
            print(state_name, num, link, pages, row_in_l_p, datetime.now(), "success")
        except Exception as e:
            print(str(e))

    driver.close()

    print(d1)

    count = 0
    for key, value in d1.items():
        url = value['link']
        pages = value['pages']
        page = pages + 1
        for j in range(1, page):
            if j == pages:
                row = value['row_in_l_p']
            else:
                row = 100

            state_name = key
            page_no = str(j)
            length = len(url)
            url_1 = url.replace(url[length - 1], str(j))
            url_2 = url_1 + "?per_page=100"
            try:
                driver = webdriver.Chrome('./chromedriver', options=options)
                driver.get(url_2)
                driver.close()
                collect.delay(row, url_2)

            except Exception as e:
                print("page", str(e))
                print("url" + url_2 + "page" + str(j) + "not extracted")
                date = datetime.now()
                state_name = key
                page_no = str(j)
                url = url_2
                extract_status = "page not extracted"
                continue


@cel.task
def collect(row, url_2):
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
    options = webdriver.ChromeOptions()
    options.add_argument(f'user-agent={user_agent}')

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

    count = 0
    driver = webdriver.Chrome('./chromedriver', options=options)
    for i in range(1, int(row) + 1):
        count = count + 1
        driver = webdriver.Chrome('./chromedriver', options=options)
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
            line = str(i), ngo_name.text.replace("\n", " "), ngo_address.text.replace("\n", " "), ngo_date.text, ngo_mobile.text, ngo_email.text
            print(line)
        except Exception as e:
            print("row", str(e))
            print("url" + url_2 + "row" + str(i) + "not extracted")
            date = datetime.now()
            row_no = str(i)
            url = url_2
            extract_status = "row not extracted"
            continue
        driver.close()


if __name__ == '__main__':
    cel.start()
