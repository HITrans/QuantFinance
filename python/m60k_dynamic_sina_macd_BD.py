# -*- coding: utf-8 -*-

#此脚为实时抓取60分钟K线数据脚本，并入 macd计算

import requests,re,json,time,urllib
import pandas as pd
import time
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from multiprocessing import Pool
from sqlalchemy import create_engine
import mylib.pachong
import datetime
import pymysql
from config import *
import mylib.macd
import mylib.gxfunctions

#创建本地数据库连接，并建立新的数据库

#db = pymysql.connect(db_host, usr_name, db_password, charset='utf8mb4')
#cursor = db.cursor()

#main_table 落数据库
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format(usr_name, db_password, db_server, 'dfcf','utf8mb4'))
con = engine.connect()#创建连接

#提取所有代码，生成DataFrame存储URL
selected_stocks = pd.read_sql('select * from dfcf.listtable',con)


#提取所有代码

stock_id = list(selected_stocks['stock_id'])
stock_id_for_url = list(selected_stocks['stock_id_sina'])
stock_name = list(selected_stocks['名称'])

url_head_sina = 'https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData?symbol='
url_tail_sina = '&scale=60&ma=no&datalen=1023'
stock_list_sina = [url_head_sina+stock_ids+url_tail_sina for stock_ids in stock_id_for_url]


#组stock_info临时dataframe
stock_info = pd.DataFrame()
stock_info['name'] = stock_name
stock_info['stock_id'] = stock_id
#stock_info['url_dfcf'] = url_list
stock_info['url_sina'] = stock_list_sina


def daily_bottom_calc(df_temp_macd_i,df_temp_xdate_i):

    #result_stock = pd.DataFrame()

    if len(df_temp_macd_i)>40:
        new_bottom_x_i = mylib.gxfunctions.goldx_now(df_temp_macd_i)

        if new_bottom_x_i is not None:
            deathx_list = [df_temp_macd_i[df_temp_macd_i['date'] == k].index.values[0].tolist() for k in
                           df_temp_xdate_i.death_date.tolist()]
            goldx_list = [df_temp_macd_i[df_temp_macd_i['date'] == k].index.values[0].tolist() for k in
                          df_temp_xdate_i.gold_date.tolist()]
            cal_result=mylib.gxfunctions.BottomDivergence(df_temp_macd_i,10, deathx_list, goldx_list)
            #print(cal_result)
            if cal_result is not None:
                cal_result.to_sql(name='m60k_result', con=con, if_exists='append', index=False)
                #print(cal_result)
                #result_stock['stock_id'] = [str(cal_result)]
            else:
                pass
        else:
            pass
    else:
        pass
    #return result




def update_macdx(df_macdx):
    #df_temp_i = df_temp[df_temp['stock_id']==stock_id].reset_index(drop=True)
    if len(df_macdx) > 40:#上市40天的股票才考虑？
        df_temp_macd = mylib.macd.get_MACD(df_macdx)#要求df_temp_i至少有40个数据
        #print(df_temp_macd)
        len_t = len(df_temp_macd)-40
        df_temp_macd_t = df_temp_macd.tail(len_t).reset_index(drop=True)  # 要求df_temp_i至少有40个数据
        df_temp_macd_t.drop(['name','volume'],axis=1,inplace=True)
        #df_temp_macd_t.to_sql(name='m30k_macd', con=con, if_exists='append', index=False)
        df_bottom_date=mylib.gxfunctions.goldx_now(df_temp_macd.tail(40))
        if df_bottom_date is not None:
            df_bottom_date_hist =mylib.gxfunctions.goldx(df_temp_macd_t)
            #df_bottom_date_hist.to_sql(name='df_bottom_m30k_hist', con=con, if_exists='append', index=False)
            daily_bottom = daily_bottom_calc(df_temp_macd_t,df_bottom_date_hist)
            #print(daily_bottom)
        else:
            pass
    else:
        pass


def build_result_df(result_temp,url):

    df_t = pd.DataFrame(result_temp)
    df_t['stock_id'] = list(stock_info.loc[stock_info['url_sina']==url,'stock_id'])[0]
    df_t['name'] = list(stock_info.loc[stock_info['url_sina']==url,'name'])[0]
    df_t = df_t[['name', 'stock_id', 'day', 'open', 'close', 'high', 'low', 'volume']]
    df_t.columns = ['name', 'stock_id', 'date', 'open', 'close', 'high', 'low', 'volume']

    return df_t

def get_stocks_mp_sina(results,url):

    if 'scale=5' in url:
        url_type = 'url_m5k'
        db_name = 'dynamicm5'
    elif 'scale=15' in url:
        url_type = 'url_m15k'
        db_name = 'dynamicm15'
    elif 'scale=30' in url:
        url_type = 'url_m30k'
        db_name = 'dynamicm30'
    elif 'scale=60' in url:
        url_type = 'url_m60k'
        db_name = 'dynamicm60'

    #tm = datetime.datetime.now()
    #close_time = tm - datetime.timedelta(minutes=tm.minute % 5,seconds=tm.second,microseconds=tm.microsecond)
    #update_time = close_time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        if results != 'null':
            results_ = json.loads(results)

            df_db = build_result_df(results_,url)
            df_db[['open', 'close', 'high', 'low','volume']] = df_db[['open', 'close', 'high', 'low','volume']].apply(pd.to_numeric)
            #df_db.to_sql(name=db_name, con=con, if_exists='append', index=False)
            update_macdx(df_db)

        else:
            pass
            #df_db = pd.DataFrame(0, index=[0],columns=['name', 'stock_id', 'date', 'open', 'close', 'high', 'low', 'volume'])
            #df_db['stock_id'] = list(stock_info.loc[stock_info['url_sina']==url,'stock_id'])[0]
            #df_db['name'] = list(stock_info.loc[stock_info['url_sina']==url,'name'])[0]
            #df_db['date'] =today

        #df_db[['open', 'close', 'high', 'low','volume']] = df_db[['open', 'close', 'high', 'low','volume']].apply(pd.to_numeric)
        #df_db.to_sql(name=db_name, con=con, if_exists='append', index=False)

    except Exception as exc:
        print('error on '+list(stock_info.loc[stock_info['url_sina']==url,'stock_id'])[0])
        print(exc)


def getHtml(url):
    #url = 'http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cb=jQuery112403755036159032368_1561627712037&type=CT&token=4f1862fc3b5e77c150a2b985b12db0fd&sty=FCOIATC&js=(%7Bdata%3A%5B(x)%5D%2CrecordsFiltered%3A(tot)%7D)&cmd=C._A&st=(ChangePercent)&sr=-1&p=1&ps=5000&_=1561627712203'
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=40))
    s.mount('https://', HTTPAdapter(max_retries=40))
    #print(time.strftime('%Y-%m-%d %H:%M:%S'))
    try:

        r = s.get(url)
        content = r.text
        get_stocks_mp_sina(content,url)
    except requests.exceptions.RequestException as e:
        print(e)
        print(time.strftime('%Y-%m-%d %H:%M:%S'))

db_name = 'dynamicm60'

def mp_job(url_k):
    data_t = getHtml(url_k)


def multicore():
    with Pool(10) as p:
        #p.map(job2, url_list)

        for _ in tqdm(p.imap_unordered(mp_job, stock_list_sina), total=len(stock_list_sina),ascii=True):
            pass

if __name__ == '__main__':

    #drop_exist_table = 'DROP TABLE IF EXISTS dfcf.dynamicm60;'
    #cursor.execute(drop_exist_table)


    multicore()
    print('60分钟k线实时数据更新及macd计算完成')