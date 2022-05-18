import json
import shutil
import os

import pyodbc as pyodbc
from selenium.webdriver.common.by import By
import requests
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
import time
import datetime
import sqlite3
from csv import writer,reader
from selenium.common.exceptions import TimeoutException
# from datetime import datetime
from os import path
from glob import glob
import logging

app_name = 'chennai_bsnl_co_in'
# all_file_path = f'D:\python\projects\{app_name}'
all_file_path = os.getcwd()
sqlite_path = f'{all_file_path}\{app_name}.db'
csv_path = f'{all_file_path}\{app_name}.csv'
log_path = f'{all_file_path}\{app_name}.log'
d_name = 'chennai.bsnl.co.inpy'
temp_down_path = os.path.expanduser('~') + '\\Documents\\pythonfiles\\' + d_name + '\\temp_files'
download_path = os.path.expanduser('~') + '\\Documents\\pythonfiles\\' + d_name + '\\files'
main_list, data_list = [], []
page_nos, skip_tenders_counts, pos = '', 0, 0
conn = sqlite3.connect(sqlite_path)
cur = conn.cursor()
if os.path.exists(log_path):
    logging.basicConfig(filename=log_path, format='%(asctime)s %(message)s', filemode='a', level=logging.INFO)
else:
    logging.basicConfig(filename=log_path, format='%(asctime)s %(message)s', filemode='w', level=logging.INFO)
if os.path.exists(all_file_path):
    pass
else:
    os.makedirs(all_file_path)
if os.path.exists(download_path):
    pass
else:
    os.makedirs(download_path)
if os.path.exists(temp_down_path):
    pass
else:
    os.makedirs(temp_down_path)

search_url = 'http://www.chennai.bsnl.co.in/tenders.php'
options = webdriver.ChromeOptions()
prefs = {'download.default_directory': temp_down_path}
options.add_argument("user-data-dir=C:\\Path")
capabilities = DesiredCapabilities.CHROME
capabilities['goog:loggingPrefs'] = {'performance': 'ALL'}
options.add_experimental_option('prefs', {
    "download.default_directory": temp_down_path,  # Change default directory for downloads
    "download.prompt_for_download": False,  # To auto download the file
    "download.directory_upgrade": True,
    "plugins.always_open_pdf_externally": True  # It will not show PDF directly in chrome
})
# driver = webdriver.Chrome(options = options,service=Service(executable_path=f"{all_file_path}\chromedriver.exe"))
driver = webdriver.Chrome(options=options, desired_capabilities=capabilities)
driver.get(search_url)
logging.info(f'Connection to {search_url}')
# driver.maximize_window()
action = ActionChains(driver)
time.sleep(1)



def new_dow(single_link):
    global d_path
    files = ''
    for j in os.listdir(temp_down_path):
        files = j
        filename, file_extension = os.path.splitext(files)
    print(f'-----{files}-----')
    if files != '' and files in single_link:
        name1 = datetime.datetime.strptime(str(datetime.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%d%m%Y_%H%M%S.%f')
        os.rename(temp_down_path + '\\' + files, temp_down_path + '\\' + 'chennai_' + name1 + file_extension)
        time.sleep(0.1)
        shutil.move(temp_down_path + '\\' + 'chennai_' + name1 + file_extension, download_path)
        # data_list.append(download_path + '\\' + 'wbiwd_' + name1 + file_extension)
        d_path = download_path + '\\' + 'chennai_' + name1 + file_extension
        logging.info(f"File download completed = {d_path}")
        print(f'download completed = {d_path}')
    else:
        time.sleep(1)
        print(f'reload because file is "{files}"')
        new_dow(single_link)
    return d_path



def a_new_dow(single_link):
    global d_path
    files = ''
    for j in os.listdir(temp_down_path):
        if files in single_link:
            files = j
            filename, file_extension = os.path.splitext(files)
            break
    print(f'-----{files}-----')
    if files != '' and files in single_link:
        name1 = datetime.datetime.strptime(str(datetime.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%d%m%Y_%H%M%S.%f')
        os.rename(temp_down_path + '\\' + files, temp_down_path + '\\' + 'chennai_' + name1 + file_extension)
        time.sleep(0.1)
        # shutil.move(temp_down_path + '\\' + 'chennai_' + name1 + file_extension, download_path)
        # data_list.append(download_path + '\\' + 'wbiwd_' + name1 + file_extension)
        d_path = download_path + '\\' + 'chennai_' + name1 + file_extension
        logging.info(f"File download completed = {d_path}")
        print(f'download completed = {d_path}')
    else:
        time.sleep(1)
        print(f'reload because file is "{files}"')
        a_new_dow(single_link)
    return d_path




def new_down_pdf(link):
    try:
        ele = driver.execute_script(f"window.open('{link}');")
        print(ele)
        time.sleep(1)
        return new_dow(link)
    except Exception as e:
        print(str(e))


def sqlite_code(main_li):
    cur.execute('INSERT INTO tenders(OpeningDate,Tender_Summery,Tender_Notice_No,Documents_2) VALUES(?,?,?,?);', main_li)
    conn.commit()
    print('Data inserted into sqlite successfully')
    logging.info(f'Data inserted successfully = {main_li}')
    cur.execute("SELECT OpeningDate,Tender_Summery,Tender_Notice_No,Documents_2 FROM tenders WHERE flag = ?", (1,))
    data2 = cur.fetchall()
    print(data2)
    # if data2 != []:
    #     with pyodbc.connect('DRIVER={SQL Server};SERVER=153TESERVER;DATABASE=CrawlingDB;UID=hrithik;PWD=hrithik@123') as conns:
    #         with conns.cursor() as cursor:
    #             q = f"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{app_name}' AND xtype='U') CREATE TABLE {app_name}(Id INTEGER PRIMARY KEY IDENTITY(1,1)\
    #                                      ,Tender_Notice_No TEXT\
    #                                      ,Tender_Summery TEXT\
    #                                      ,Tender_Details TEXT\
    #                                      ,Bid_deadline_2 TEXT\
    #                                      ,Documents_2 TEXT\
    #                                      ,OpeningDate TEXT\
    #                                      ,TenderListing_key TEXT\
    #                                      ,Notice_Type TEXT\
    #                                      ,Competition TEXT\
    #                                      ,Purchaser_Name TEXT\
    #                                      ,Pur_Add TEXT\
    #                                      ,Pur_State TEXT\
    #                                      ,Pur_City TEXT\
    #                                      ,Pur_Country TEXT\
    #                                      ,Pur_Email TEXT\
    #                                      ,Pur_URL TEXT\
    #                                      ,Bid_Deadline_1 TEXT\
    #                                      ,Financier_Name TEXT\
    #                                      ,CPV TEXT\
    #                                      ,scannedImage TEXT\
    #                                      ,Documents_1 TEXT\
    #                                      ,Documents_3 TEXT\
    #                                      ,Documents_4 TEXT\
    #                                      ,Documents_5 TEXT\
    #                                      ,currency TEXT\
    #                                      ,actualvalue TEXT\
    #                                      ,TenderFor TEXT\
    #                                      ,TenderType TEXT\
    #                                      ,SiteName TEXT\
    #                                      ,createdOn TEXT\
    #                                      ,updateOn TEXT\
    #                                      ,Content TEXT\
    #                                      ,Content1 TEXT\
    #                                      ,Content2 TEXT\
    #                                      ,Content3 TEXT\
    #                                      ,DocFees TEXT\
    #                                      ,EMD TEXT\
    #                                      ,Tender_No TEXT)"
    #             cursor.execute(q)
    #             conns.commit()
    #             q = f"INSERT INTO {app_name}(OpeningDate,Tender_Summery,Tender_Notice_No,Documents_2) VALUES(?,?,?,?)"
    #             cursor.execute(q, data2[0])
    #             logging.info(f'Data inserted on server')
    #             print(f'Data inserted on server', '\n')
    #     sql1 = f'UPDATE tenders SET flag ={0} WHERE flag = {1};'
    #     cur.execute(sql1)
    #     conn.commit()
    #     logging.info(f'Flag updated')
    # else:
    #     print(f'Data already available in sqlite database')
    #     logging.info(f'Data already available in sqlite database')


def scraping_code():
    length_of_tr = WebDriverWait(driver, 4).until(EC.presence_of_all_elements_located((By.XPATH, f'/html/body/div/ul/li')))
    for p, i in enumerate(length_of_tr):
        data_list = []
        da = str(i.text).lower()

        if 'corrigendum' in da or 'corrigemdum' in da:
            logging.info(f'Skip tender because of "corrigendum" = {da}')
            print('skip tender because of "corrigendum"', '\n')
        else:
            Opening_Date = WebDriverWait(i, 4).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div/ul/li[{p + 1}]/div/div[2]/p[3]'))).text
            OpeningDate = str(datetime.datetime.strptime(Opening_Date.replace('Opening Date : - ', '').split(' ')[0], "%d/%m/%Y").strftime('%d-%m-%Y')).split(' ')[0]
            data_list.append(OpeningDate)
            print(OpeningDate)

            Tender_Summery = WebDriverWait(i, 4).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div/ul/li[{p + 1}]/div/div[2]/h4/a/font'))).text
            data_list.append(Tender_Summery)
            print(Tender_Summery)

            Tender_Notice_No = WebDriverWait(i, 4).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div/ul/li[{p + 1}]/div/div[2]/p[1]'))).text.replace('NIQ NO. : ', '')
            data_list.append(Tender_Notice_No)
            print(Tender_Notice_No)

            cur.execute("SELECT Tender_Notice_No FROM tenders WHERE OpeningDate = ? and Tender_Summery = ? and Tender_Notice_No = ?",(OpeningDate, Tender_Summery, Tender_Notice_No))
            a = cur.fetchone()

            if a is None:
                try:
                    Documents_2 = WebDriverWait(i, 4).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div/ul/li[{p + 1}]/div/div[2]/span/a')))
                    print(Documents_2.get_attribute('href'))

                    r = requests.get(Documents_2.get_attribute('href'))
                    if r.status_code != 404:
                        driver.execute_script(f"window.open('{Documents_2.get_attribute('href')}');")
                        # time.sleep(1)
                        data_list.append(new_dow(Documents_2.get_attribute('href')))
                        sqlite_code(data_list)
                        logging.info(f'Scraped data = {data_list}')
                    else:
                        data_list.append('')
                        sqlite_code(data_list)
                        logging.info(f'Scraped data = {data_list}')
                except StaleElementReferenceException as se:
                    print(f'{str(se)}')
            else:
                logging.info(f'Data already available = {a}')
                print('Data already available', '\n')


try:
    sql = """  CREATE TABLE IF NOT EXISTS tenders(Id INTEGER PRIMARY KEY AUTOINCREMENT
                                                             ,Tender_Notice_No TEXT
                                                             ,Tender_Summery TEXT
                                                             ,Tender_Details TEXT
                                                             ,Bid_deadline_2 TEXT
                                                             ,Documents_2 TEXT
                                                             ,OpeningDate TEXT
                                                             ,TenderListing_key TEXT
                                                             ,Notice_Type TEXT
                                                             ,Competition TEXT
                                                             ,Purchaser_Name TEXT
                                                             ,Pur_Add TEXT
                                                             ,Pur_State TEXT
                                                             ,Pur_City TEXT
                                                             ,Pur_Country TEXT
                                                             ,Pur_Email TEXT
                                                             ,Pur_URL TEXT
                                                             ,Bid_Deadline_1 TEXT
                                                             ,Financier_Name TEXT
                                                             ,CPV TEXT
                                                             ,scannedImage TEXT
                                                             ,Documents_1 TEXT
                                                             ,Documents_3 TEXT
                                                             ,Documents_4 TEXT
                                                             ,Documents_5 TEXT
                                                             ,currency TEXT
                                                             ,actualvalue TEXT
                                                             ,TenderFor TEXT
                                                             ,TenderType TEXT
                                                             ,SiteName TEXT
                                                             ,createdOn TEXT
                                                             ,updateOn TEXT
                                                             ,Content TEXT
                                                             ,Content1 TEXT
                                                             ,Content2 TEXT
                                                             ,Content3 TEXT
                                                             ,DocFees TEXT
                                                             ,EMD TEXT
                                                             ,Tender_No TEXT
                                                             ,flag INT DEFAULT 1);  """
    cur.execute(sql)
    conn.commit()
    scraping_code()

except WebDriverException as wd:
    print(f'{str(wd)}')
    logging.error(f'{str(wd)}')
except Exception as e:
    print(f'Page Loop {e}')
    logging.error(f'{str(e)}')
    driver.quit()