# -*- coding: utf-8 -*-

#此脚为实时抓取15分钟K线数据脚本，设计目标每5分钟自动运行一次，落入动态数据库以及历史数据库

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

db = pymysql.connect(db_host, usr_name, db_password, charset='utf8mb4')
cursor = db.cursor()

#main_table 落数据库
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format(usr_name, db_password, db_server, 'dfcf','utf8mb4'))
con = engine.connect()#创建连接

#生成全盘总表
url_main = 'http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cb=jQuery112403755036159032368_1561627712037&type=CT&token=4f1862fc3b5e77c150a2b985b12db0fd&sty=FCOIATC&js=(%7Bdata%3A%5B(x)%5D%2CrecordsFiltered%3A(tot)%7D)&cmd=C._A&st=(ChangePercent)&sr=-1&p=1&ps=5000'
stocks = mylib.pachong.getStock(url_main)
main_table = pd.DataFrame(stocks)
#提取主要数据/提取全部数据
stock_id = list(main_table.iloc[:, 1])
stock_mk = list(main_table.iloc[:, 0])
stock_id_mk = [m+str(n) for m,n in zip(stock_id,stock_mk)]
main_table['mid'] = stock_id_mk
main_table.drop([0,14,15,16,17,18,19,20,21,22,23,25],axis=1,inplace=True)
columns = {1:"代码",2:"名称",3:"最新价格",4:"涨跌额",5:"涨跌幅",6:"成交量",7:"成交额",8:"振幅",9:"最高",10:"最低",11:"今开",12:"昨收",13:"量比",24:"时间"}
main_table.rename(columns = columns,inplace=True)

#提取所有代码
stock_id = list(main_table['代码'])
stock_name = list(main_table['名称'])
stock_id_for_url = list(main_table['mid'])
url_head = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=m5k&style=top&num=2&rtntype=1&id='
url_list = [url_head+stock_ids for stock_ids in stock_id_for_url]

stock_id_sina = []
for i in range(len(stock_id)):
    if stock_mk[i] =='1':
        stock_id_temp = 'sh'+str(stock_id[i])
        stock_id_sina.append(stock_id_temp)
    else:
        stock_id_temp = 'sz'+str(stock_id[i])
        stock_id_sina.append(stock_id_temp)

url_head_sina = 'https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData?symbol='
url_tail_sina = '&scale=15&ma=no&datalen=41'
stock_list_sina = [url_head_sina+stock_ids+url_tail_sina for stock_ids in stock_id_sina]


#组stock_info临时dataframe
stock_info = pd.DataFrame()
stock_info['name'] = stock_name
stock_info['stock_id'] = stock_id
stock_info['url_dfcf'] = url_list
stock_info['url_sina'] = stock_list_sina

stocks_m5k = pd.DataFrame()
stocks_m5k_list = []

def update_macdx(df_macdx):
    #df_temp_i = df_temp[df_temp['stock_id']==stock_id].reset_index(drop=True)
    if len(df_macdx) > 40:#上市40天的股票才考虑？
        df_temp_macd = mylib.macd.get_MACD(df_macdx)#要求df_temp_i至少有40个数据
        df_temp_macd_t = df_temp_macd.tail(1).reset_index(drop=True)  # 要求df_temp_i至少有40个数据
        df_temp_macd_t.drop(['name','volume'],axis=1,inplace=True)
        df_temp_macd_t.to_sql(name='m15k_macd', con=con, if_exists='append', index=False)
        df_bottom_date=mylib.gxfunctions.goldx_now(df_temp_macd)
        if df_bottom_date is not None:
            df_bottom_date.to_sql(name='m15k_xdate', con=con, if_exists='append', index=False)

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
            df_db.to_sql(name=db_name, con=con, if_exists='append', index=False)
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

db_name = 'dynamicm15'

def mp_job(url_k):
    data_t = getHtml(url_k)


def multicore():
    with Pool(10) as p:
        #p.map(job2, url_list)

        for _ in tqdm(p.imap_unordered(mp_job, stock_list_sina), total=len(stock_list_sina)):
            pass

if __name__ == '__main__':

    drop_exist_table = 'DROP TABLE IF EXISTS dfcf.dynamicm15;'
    cursor.execute(drop_exist_table)


    multicore()
    print('15分钟k线实时数据更新及macd计算完成')