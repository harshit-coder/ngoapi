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
    def collect(self, url, i, op, wb, sh1, filename):
        print(str(i + 1))
        driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), options=op)
        driver.get(url)
        driver.implicitly_wait(10)
        all = driver.find_element_by_xpath("/html/body/div[9]/div[1]/div[3]/div/div/div[2]/table/tbody/tr[" + str(i + 1) + "]/td[2]/a")
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
        # print("address:", add.text)
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
        driver.close()
        return line


@app.route('/')
def test():
    return render_template("form.html")


@app.route('/scrape', methods=["GET", "POST"])
def scrape():
    op = webdriver.ChromeOptions()
    op.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
    op.add_argument("--headless")
    op.add_argument("--no-sandbox")
    op.add_argument("--disable-dev-sh-usage")
    wb = Workbook()
    sh1 = wb.active
    row = sh1.max_row
    column = sh1.max_column
    sh1.cell(row=1, column=1, value="name")
    sh1.cell(row=1, column=2, value="regdate")
    sh1.cell(row=1, column=3, value="address")
    sh1.cell(row=1, column=4, value="phone no")
    sh1.cell(row=1, column=5, value="email")
    if request.method == "POST":
        url = request.form.get("url")
        rows = request.form.get("Number")
        filename = request.form.get("page")
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(rows)) as executor:
            future_to_url = {executor.submit(ngo.collect, url, i, op, wb, sh1, filename): i for i in range(int(rows))}

            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))
                else:
                    print('%r page is %d bytes' % (url, len(data)))

        response = send_file(filename + ".xls")
        return response


ngo = NGO()
if __name__ == "__main__":
    app.run(debug=True)
