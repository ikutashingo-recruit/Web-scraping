from selenium import webdriver
from selenium.webdriver.chrome import service as fs
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta, timezone

import pandas as pd
import time
import os
import re
import glob

os.chdir(os.path.dirname(os.path.realpath(__file__)))

import sys
sys.path.append("../../module")
from chatwork import chatwork #チャットワークに飛ばす
from apicheck import error_check #エラーシステムに飛ばす

sys.path.append("../../")
from config import CREDENTIALS,FC2_SCRPY_SETTING,PROFILE_PATH,PROFILE_NAME

options = webdriver.chrome.options.Options()
profile_path = r"/home/ubuntu/.config/google-chrome/"


programname = 'fc2'

JST = timezone(timedelta(hours=+9), 'JST')
dt_now = datetime.now(JST)
now = dt_now.strftime('%Y-%m-%d %H:%M:%S')
date = dt_now.strftime('%Y-%m-%d')
yesterday = dt_now-timedelta(days=1)
yesterday1 = yesterday.strftime('%Y-%m-%d')

table_schema = [{'name': 'date', 'type': 'DATE'},
                {'name': 'product', 'type': 'STRING'},
                {'name': 'price', 'type': 'INTEGER'},
                {'name': 'income', 'type': 'INTEGER'},
                {'name': 'count', 'type': 'INTEGER'},
                {'name': 'sitename', 'type': 'STRING'}]

gcftitle = programname+"::data_uraaka_scrpy"

url= "https://fc2.com/login.php?ref=merchant"
url1= "https://merchant.fc2.com/member/sales/history.php?from_sell_date="+str(yesterday1)+"&to_sell_date="+str(yesterday1)



if __name__ == '__main__':

    sql = "SELECT sitename FROM `テーブルデータ` WHERE date = '"+ yesterday1 +"'"
    

    acquired_list = pd.read_gbq(sql, dialect='standard',credentials=CREDENTIALS)
    acquired_list = acquired_list['sitename'].tolist()

    for setdata in FC2_SCRPY_SETTING:
        
        if not (setdata['sitename'] in acquired_list):
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--user-data-dir=' + PROFILE_PATH)
            chrome_options.add_argument('--profile-directory=' + PROFILE_NAME)
            google_service = fs.Service(executable_path="../../chromedriver")
            driver = webdriver.Chrome(service = google_service,options=chrome_options)
            driver.implicitly_wait(10)

            result = pd.DataFrame(columns=['date','product','price','income','count','sitename'])


            driver.get(url)
            time.sleep(5)
            try :
                element = driver.find_element(By.CSS_SELECTOR,"input[type=image]") 
                element.click() #login
                time.sleep(5)

            except :
                pass
            

            time.sleep(9)
            driver.get(url1)
            driver.implicitly_wait(10)

            non_text = driver.find_element(By.XPATH,'//*[@id="contents"]/div[2]').text

            if non_text == "検索条件に一致する売上履歴は見つかりませんでした。":
                chatwork(dt_now.strftime('%Y-%m-%d')+gcftitle+'\nhas not been saled',000000000)
                error_check(gcftitle,1,gcftitle +'BQ送信(empty)に成功しました')
                driver.quit()
                exit()

            else:
                elem_table = driver.find_element(By.CSS_SELECTOR,"table.table_list")
                html = elem_table.get_attribute('outerHTML')
                dfs = pd.read_html(html,header=0)[0]

                df_data = dfs.drop(['品番','販売者名','形式','売上額','アフィリエイト費用'],axis=1)
                df_data = df_data.rename(columns={'売上日時':'date','タイトル(商品名)':'product','販売額':'price','利益額':'income'})
                try:
                    df_data['price'] = df_data['price'].replace(",","",regex=True)                    
                except:
                    pass
                df_data['price'] = df_data['price'].str.extract(r'(\d+)pt').astype(int)
                try:
                    df_data['income'] = df_data['income'].replace(",","",regex=True)
                except:
                    pass
                df_data['income'] = df_data['income'].str.extract(r'(\d+)pt').astype(int)

                df_data['sitename'] = programname
                df_data['count'] = df_data['income']/df_data['price']/0.55
                df_data['count'] = df_data['product'].map(df_data['product'].value_counts())
                df_data['income'] = df_data['income'] * df_data['count']
                df_data2 = df_data.drop_duplicates(subset='product')
                df_data2['date'] = pd.to_datetime(df_data2['date'],format='%Y-%m-%d')
                

                try:
                    df_data2['date'] = df_data2['date'].dt.strftime('%Y-%m-%d')


                    df_data2.to_gbq("テーブル名", project_id="ID名",credentials=CREDENTIALS, if_exists="append",table_schema=table_schema)
                    chatwork(dt_now.strftime('%Y-%m-%d')+programname+'\nhas been sent',000000000)
                    error_check(gcftitle,1,gcftitle +'BQ送信に成功しました')
                except:
                    chatwork(dt_now.strftime('%Y-%m-%d')+'\n'+ gcftitle +'\nのBQ送信に失敗しました',000000000)
                    error_check(gcftitle,0,gcftitle +'BQ送信に失敗しました')
                finally:
                    driver.quit()
