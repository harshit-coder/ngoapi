from bs4 import BeautifulSoup as soup
from openpyxl import Workbook
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import os

print("Keep your net connection on and don't do anything when window open or closes")
print("/n")
url =str(input("Enter the URL : "))
print("/n")
rows = int(input("Enter the rows in that URL page table  : "))
print("/n")
filename = str(input("Enter that page number of which you are copying data  : "))
print("/n")
print("please keep your internet connected , Wait for 30 -40 min  your excel sheet will be uploaded on the location which is defined in your windows command prompt, don't close the command prompt, and keep an eye if program stops take a screenshot and send on group ")

wb  = Workbook()
sh1 = wb.active
row = sh1.max_row
column = sh1.max_column
sh1.cell(row = 1, column = 1, value = "name")
sh1.cell(row = 1, column = 2, value = "regdate")
sh1.cell(row = 1, column = 3, value = "address")
sh1.cell(row = 1, column = 4, value = "phone no")
sh1.cell(row = 1, column = 5, value = "email")

op = webdriver.ChromeOptions()
op.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
op.add_argument("--headless")
op.add_argument("--no-sandbox")
op.add_argument("--disable-dev-sh-usage")

for i in range(rows):
    print(str(i+1))
    driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=op)
    driver.get(url)
    driver.implicitly_wait(10)
    all =  driver.find_element_by_xpath("/html/body/div[9]/div[1]/div[3]/div/div/div[2]/table/tbody/tr["+str(i+1)+"]/td[2]/a")
    all.click()
    overlay = WebDriverWait(driver, 10).until(EC.invisibility_of_element_located((By.CSS_SELECTOR,"div.blockUI.blockOverlay")))
    if overlay:
        close = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//*[@id='ngo_info_modal']/div[2]/div/div[1]/button/span")))
        close.click()
    ngo = driver.find_element_by_id('ngo_info_modal')
    result = ngo.get_attribute( 'innerHTML' )
    sp = soup( result , 'html.parser' )
    name= sp.find (id = "ngo_name_title" )
    add= sp.find (id = "address" )
    date= sp.find (id = "ngo_reg_date" )
    mobile= sp.find (id = "mobile_n" )
    email= sp.find (id = "email_n" )
    print("row",str(i+1))
    print("name:",name.text)
    print("add:",add.text)
    print("date",date.text)
    print("mobile",mobile.text)
    print("email",email.text)
    sh1.cell(row = i+1, column = 1, value = name.text)
    sh1.cell(row = i+1, column = 2, value = date.text)
    sh1.cell(row = i+1, column = 3, value = add.text)
    sh1.cell(row = i+1, column = 4, value = mobile.text)
    sh1.cell(row = i+1, column = 5, value = email.text)
    wb.save(filename+".xls")
    driver.close()
print("pLease verify data of excel sheet from the page")
