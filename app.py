import concurrent.futures
import os
from bs4 import BeautifulSoup as soup
from flask import Flask, render_template, request
from flask import send_file
from openpyxl import Workbook
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

app = Flask(__name__)


class NGO:
    def collect(self, url, i, options, wb, sh1, filename, driver):
        print(str(i))
        driver.get(url)
        driver.implicitly_wait(10)
        all = driver.find_element_by_xpath("/html/body/div[9]/div[1]/div[3]/div/div/div[2]/table/tbody/tr[" + str(i) + "]/td[2]/a")
        all.click()
        overlay = WebDriverWait(driver, 10).until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "div.blockUI.blockOverlay")))
        if overlay:
            close = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//*[@id='ngo_info_modal']/div[2]/div/div[1]/button/span")))
            close.click()
        ngo = driver.find_element_by_id('ngo_info_modal')
        result = ngo.get_attribute('innerHTML')
        sp = soup(result, 'html.parser')
        name = sp.find(id="ngo_name_title")
        add = sp.find(id="address")
        date = sp.find(id="ngo_reg_date")
        mobile = sp.find(id="mobile_n")
        email = sp.find(id="email_n")
        # print("row", str(i + 1))
        # print("name:", name.text)
        # print("add:", add.text)
        # print("date", date.text)
        # print("mobile", mobile.text)
        # print("email", email.text)
        line = str(i + 1), name.text, add.text, date.text, mobile.text, email.text
        print(line)
        sh1.cell(row=i + 1, column=1, value=name.text)
        sh1.cell(row=i + 1, column=2, value=date.text)
        sh1.cell(row=i + 1, column=3, value=add.text)
        sh1.cell(row=i + 1, column=4, value=mobile.text)
        sh1.cell(row=i + 1, column=5, value=email.text)
        wb.save(filename + ".xls")


@app.route('/')
def test():
    return render_template("form.html")


@app.route('/scrape', methods=["GET", "POST"])
def scrape():
    options = webdriver.ChromeOptions()
    options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
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
    driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=options)
    wb = Workbook()
    sh1 = wb.active
    row = sh1.max_row
    column = sh1.max_column
    sh1.cell(row=1, column=1, value="name")
    sh1.cell(row=1, column=2, value="regdate")
    sh1.cell(row=1, column=3, value="address")
    sh1.cell(row=1, column=4, value="phone no")
    sh1.cell(row=1, column=5, value="email")
    url = request.form.get("url")
    start = request.form.get("start")
    rows = request.form.get("Number")
    filename = request.form.get("page")
    wb.save(filename + ".xls")
    if request.method == "POST":
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(rows)+1) as executor:
            future_to_url = {executor.submit(ngo.collect, url, i, options, wb, sh1, filename, driver): i for i in range(int(start), int(rows) + 1)}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                print("url", url)
                try:
                    data = future.result()
                    print("data", data)
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))
                else:
                    print('%r page is %d bytes' % (url, len(data)))

            response = "success"
            return response


ngo = NGO()
if __name__ == "__main__":
    app.run(debug=True)
