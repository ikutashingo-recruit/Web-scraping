from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta, timezone

import pandas as pd
import time
import os
import re

os.chdir(os.path.dirname(os.path.realpath(__file__)))

programname = os.path.basename(os.path.dirname(__file__)) #プログラムがあるフォルダ名

import sys
sys.path.append("../../module")
from chatwork import chatwork　#チャットワークに飛ばす自作ライブラリ
from apicheck import error_check　#エラーチェックシステムに飛ばす自作ライブラリ

sys.path.append("../../")
from config import CREDENTIALS,TIKTOK_FOLLOWER_SETTING,TALENT_NAME,PROFILE_PATH,PROFILE_NAME

if __name__ == '__main__':
    

    table_schema = [{'name': 'date', 'type': 'DATE'},
                    {'name': 'type', 'type': 'STRING'},
                    {'name': 'name', 'type': 'STRING'},
                    {'name': 'count', 'type': 'INTEGER'}]
    
    tiktok_url = "https://www.tiktok.com/"

    JST = timezone(timedelta(hours=+9), 'JST')
    dt_now = datetime.now(JST)
    df = pd.DataFrame(columns=['date','type','name','count'])
    result = pd.DataFrame(columns=['date','type','name','count'])
    sql = "SELECT name FROM `テーブルデータ` WHERE type ='tiktok' and date = '"+ dt_now.strftime('%Y-%m-%d') +"'"
    acquired_list = pd.read_gbq(sql, dialect='standard',credentials=CREDENTIALS)
    acquired_list = acquired_list['name'].tolist()

    for setdata in TIKTOK_FOLLOWER_SETTING:
        gcftitle = setdata['name']+"::tiktok_follower_scrpy"
        if not (setdata['name'] in acquired_list):
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--user-data-dir=' + PROFILE_PATH)
            chrome_options.add_argument('--profile-directory=' + setdata["profile"])
            driver = webdriver.Chrome("../../chromedriver",options=chrome_options)
            driver.implicitly_wait(10)

            driver.get(tiktok_url+"@"+setdata['id'])
            posts = driver.find_elements(By.CSS_SELECTOR,'strong[title="フォロワー"]')
            if 'K' in posts[0].text:
                driver.get(tiktok_url + "analytics?lang=ja-JP&type=business_suite&tab=Overview")
                time.sleep(10)
                posts = driver.find_elements(By.CSS_SELECTOR,'.chart-title')
                result_count = 0
                for post in posts:
                    count = re.sub(r'合計\n.*', '', post.text)
                    count = re.sub(r'\D', '', count)
                    if(count):
                        result_count = count

                if(result_count):
                    result = result.append({'date':dt_now,'name':setdata['name'],'type':'tiktok','count':result_count},ignore_index=True)

            else:
                posts = driver.find_elements(By.CSS_SELECTOR,'strong[title="フォロワー"]')
                if (len(posts)):
                    count = posts[0].text
                    result = result.append({'date':dt_now,'name':setdata['name'],'type':'tiktok','count':count},ignore_index=True)



        time.sleep(2)
        driver.quit()
    
    if not (result.empty):
        result['count'] = result['count'].astype(int)
        result['date'] = pd.to_datetime(result['date'])
        result['date'] = result['date'].dt.strftime('%Y-%m-%d')
        try:
            result.to_gbq("テーブルデータ", project_id="IDデータ",credentials=CREDENTIALS, if_exists="append",table_schema=table_schema)
            error_check(gcftitle,1,'tiktokのBQ送信に成功しました')
        except:
            chatwork(dt_now.strftime('%Y-%m-%d')+'\n'+ setdata['name'] +'\nのBQ送信に失敗しました',000000000)
            error_check(setdata['name'],0,'のBQ送信に失敗しました')
