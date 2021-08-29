import os
import time
from datetime import datetime
from telnetlib import EC

from bs4 import BeautifulSoup as soup
from celery import Celery
from selenium import webdriver
from selenium.webdriver.chrome import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


cel = Celery('tasks', backend="rpc://guest:guest@localhost:15672", broker="amqp://guest:guest@localhost:15672")


@cel.task
def collect_state(options):
    count = 0
    driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=options)
    driver.get("https://ngodarpan.gov.in/index.php/home/statewise")
    states = driver.find_elements_by_class_name("bluelink11px")
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
            state_info.state_name = state_name[0]
            state_info.number = num
            state_info.url = link
            state_info.pages = pages
            state_info.row_in_l_p = row_in_l_p
            state_info.date = datetime.now()
            state_info.extract_status = "success"
            state_details = session.query(States).filter(States.state_name == state_info.state_name, States.url == state_info.url, States.number == state_info.number).first()
            if state_details is None:
                new_state_info = States(**state_info.dict())
                session.add(new_state_info)
                session.commit()
                session.refresh(new_state_info)
            else:
                print(state_info.state_name + "state" + str(state_info.number) + "number of rows with " + state_info.url + "link is already there")
        except Exception as e:
            print("Exception", str(e))
            print(state, "not extracted")
            continue
    driver.close()
    return count


@cel.task
def collect(options):
    l1 = []
    d1 = {}
    count = 0
    states = session.query(States).all()
    for state in states:
        url = state.url
        pages = state.pages
        page = pages + 1
        for j in range(1, page):
            if j == pages:
                row = state.row_in_l_p
            else:
                row = 100

            ngo_info.state_name = state.state_name
            ngo_info.page_no = str(j)
            length = len(url)
            url_1 = url.replace(url[length - 1], str(j))
            url_2 = url_1 + "?per_page=100"
            try:
                driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=options)
                driver.get(url_2)
                driver.close()
                for i in range(1, int(row) + 1):
                    count = count + 1
                    driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=options)
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
                        ngo_info.ngo_name = sp.find(id="ngo_name_title")
                        ngo_info.ngo_address = sp.find(id="address")
                        ngo_info.ngo_date = sp.find(id="ngo_reg_date")
                        ngo_info.ngo_mobile = sp.find(id="mobile_n")
                        ngo_info.ngo_email = sp.find(id="email_n")
                        ngo_info.row_no = str(i)
                        ngo_info.date = datetime.now()
                        line = str(i), ngo_info.ngo_name, ngo_info.ngo_address, ngo_info.ngo_date, ngo_info.ngo_mobile, ngo_info.ngo_email
                        print(line)
                        ngo_info.extract_status = "success"
                        ngo_details = session.query(NgoData).filter(NgoData.ngo_name == ngo_info.ngo_name, NgoData.ngo_address == ngo_info.ngo_address, NgoData.ngo_mobile == ngo_info.ngo_mobile, NgoData.ngo_date == ngo_info.ngo_date, NgoData.ngo_email == ngo_info.ngo_email).first()
                        if ngo_details is None:
                            new_ngo_info = NgoData(**ngo_info.dict())
                            session.add(new_ngo_info)
                            session.commit()
                            session.refresh(new_ngo_info)
                        else:
                            print("state:" + state.state_name + "page_no" + str(i) + "row" + str(j) + "ngo" + ngo_info.ngo_name + "already extracted")
                    except Exception as e:
                        print("row", str(e))
                        print("url" + url_2 + "row" + str(i) + "not extracted")
                        ngo_info.date = datetime.now()
                        ngo_info.row_no = str(i)
                        ngo_info.url = url_2
                        ngo_info.extract_status = "row not extracted"
                        new_ngo_info = NgoData(**ngo_info.dict())
                        session.add(new_ngo_info)
                        session.commit()
                        session.refresh(new_ngo_info)
                        continue
                    driver.close()
            except Exception as e:
                print("page", str(e))
                print("url" + url_2 + "page" + str(j) + "not extracted")
                ngo_info.date = datetime.now()
                ngo_info.state_name = state.state_name
                ngo_info.page_no = str(j)
                ngo_info.url = url_2
                ngo_info.extract_status = "page not extracted"
                new_ngo_info = NgoData(**ngo_info.dict())
                session.add(new_ngo_info)
                session.commit()
                session.refresh(new_ngo_info)
                continue
    return count


if __name__ == '__main__':
    cel.start()
