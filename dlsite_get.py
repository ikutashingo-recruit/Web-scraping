from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.chrome import service as fs
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta, timezone

import pandas as pd
import time
import os
import re
import glob
import signal

os.chdir(os.path.dirname(os.path.realpath(__file__)))

import sys
sys.path.append("../../module")
from chatwork import chatwork #自作ライブラリ（チャットワークに結果を送信）
from apicheck import error_check #自作ライブラリ（チャットワークに結果を送信）

sys.path.append("../../")
from config import CREDENTIALS,DLSITE_SCRPY_SETTING,PROFILE_PATH,PROFILE_NAME #自作ライブラリ（プロフィールまとめ）

options = webdriver.chrome.options.Options()
profile_path = r"/home/ubuntu/.config/google-chrome/"

dl_path = r"../../tmp/sales.csv"

programname = 'dlsite'

JST = timezone(timedelta(hours=+9), 'JST')
dt_now = datetime.now(JST)
now = dt_now.strftime('%Y-%m-%d %H:%M:%S')
date = dt_now.strftime('%Y-%m-%d')
yesterday = dt_now-timedelta(days=1)
yesterday1 = yesterday.strftime('%Y-%m-%d')

if __name__ == '__main__':
    gcftitle = programname+"::data_uraaka_scrpy"

    table_schema = [{'name': 'date', 'type': 'DATE'},
                    {'name': 'product', 'type': 'STRING'},
                    {'name': 'price', 'type': 'INTEGER'},
                    {'name': 'income', 'type': 'INTEGER'},
                    {'name': 'count', 'type': 'INTEGER'},
                    {'name': 'sitename', 'type': 'STRING'}]

    url= "https://login.dlsite.com/login?user=self"
    url1= "https://www.dlsite.com/circle/circle/sale/result"

    JST = timezone(timedelta(hours=+9), 'JST')
    dt_now = datetime.now(JST)
    result = pd.DataFrame(columns=['date','type','name','count'])
    sql = "SELECT sitename FROM `digtal-eyes-cloud.youtube_data.uraaka_data` WHERE date = '"+ yesterday1 +"'"
    acquired_list = pd.read_gbq(sql, dialect='standard',credentials=CREDENTIALS)
    acquired_list = acquired_list['sitename'].tolist()

    for setdata in DLSITE_SCRPY_SETTING:
        
        if not (setdata['sitename'] in acquired_list):
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--user-data-dir=' + PROFILE_PATH)
            chrome_options.add_argument('--profile-directory=' + PROFILE_NAME)
            google_service = fs.Service(executable_path="../../chromedriver")
            driver = webdriver.Chrome(service = google_service,options=chrome_options)
            driver.implicitly_wait(10)
            try:
                driver.get(url)
                time.sleep(5)
                try :
                    element = driver.find_element(By.CSS_SELECTOR,"button.btn.type-sizeMd") 
                    element.click() #login
                    time.sleep(5)
                except :
                    pass


                driver.get(url1)
                driver.implicitly_wait(10)

                term_object = driver.find_element(By.ID,"term_type")
                term_select = Select(term_object)
                term_select.select_by_value('yesterday') #decide
                time.sleep(2)
                display_element = driver.find_element(By.CSS_SELECTOR,"input#search")
                display_element.click() #display
                time.sleep(3)
                dl_element = driver.find_element(By.CSS_SELECTOR,'input#csv')
                dl_element.click()#download
                timeout = 0
                while True:
                    glist = glob.glob(dl_path)
                    if(len(glist) > 0):
                        break
                    timeout += 1
                    if(timeout>120):
                        chatwork(dt_now.strftime('%Y-%m-%d')+programname+'\nhas been faild',000000000)
                        error_check(gcftitle,0,gcftitle+'has been faild')

                        driver.quit()
                        exit()
                    time.sleep(1)
            except:
                chatwork(dt_now.strftime('%Y-%m-%d')+programname+'\nhas been faild',000000000)
                error_check(gcftitle,0,gcftitle+'has been faild')
            driver.quit()
            glist = glob.glob(dl_path)
            for g in glist:
                dl_df = pd.read_csv(g,encoding='shift-jis')
                sale_df = dl_df[:-1] #lastrow delete
                df_sql = sale_df.loc[:,['作品名','販売価格','販売数','売上額']]
                df_sql['date'] = yesterday1
                df_sql = df_sql.rename(columns={'販売数':'count','作品名':'product','販売価格':'price','売上額':'income'})
                df_sql['sitename'] = programname
                
                if len(df_sql.index)==0:
                    chatwork(dt_now.strftime('%Y-%m-%d')+programname+'\nhas not exist',000000000)
                    error_check(gcftitle,1,gcftitle +'BQ送信(empty)に成功しました')
                    os.remove(g)
                else:
                    df_sql.to_gbq("**********.*********", project_id="***********",credentials=CREDENTIALS, if_exists="append",table_schema=table_schema)
                    chatwork(dt_now.strftime('%Y-%m-%d')+programname+'\nhas been sent',000000000)
                    error_check(gcftitle,1,gcftitle +'BQ送信に成功しました')
                    os.remove(g)
                
