#此脚为实时抓取5分钟K线数据脚本，设计目标每5分钟自动运行一次，落入动态数据库以及历史数据库

import pandas as pd
#import os
#import requests,re,json,time,urllib
#import heapq
#from bs4 import BeautifulSoup
#import sys
import pymysql
from sqlalchemy import create_engine
import mylib.sql_lib
import mylib.pachong
import time
from tqdm import tqdm
start = time.time()

#创建本地数据库连接
con = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='dfcf', port = 3306) # 连接
cur = con.cursor()

#main_table 落数据库
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', '123456', '127.0.0.1:3306', 'dfcf','utf8'))
conn = engine.connect()#创建连接

#从数据库main_table读取股票代码信息
sql_t = 'select 名称,代码,mid from main_table'
df = mylib.sql_lib.read_table(cur, sql_t)

stock_id = list(df.iloc[:, 1])
stock_id_for_url = list(df.iloc[:, 2])
stock_name = list(df.iloc[:, 0])

#生成历史数据函数，并保存入mysql数据库
def get_hist_daily(stock_id_for_url,url,db_name):
    err_stock_id = []
    err_stock_id_for_url = []
    df_temp = pd.DataFrame()
    for i in tqdm(range(len(stock_id_for_url))):
        while True:
            try:
                url_t = url+stock_id_for_url[i]
                result_temp = mylib.pachong.get_stock_hist(url_t)
                result_temp['stock_id'] = str(stock_id[i])
                result_temp['name'] = str(stock_name[i])
                if len(result_temp['stock_id'])==0:
                    result_temp = pd.DataFrame(0, index=[0],columns=['name', 'stock_id', 'date', 'open', 'close', 'high', 'low', 'deal', 'dealprice', 'amp'])
                    result_temp['stock_id'] = str(stock_id[i])
                    result_temp['name'] = str(stock_name[i])
                else:
                    pass
                result_temp = result_temp[['name', 'stock_id', 'date', 'open', 'close', 'high', 'low', 'deal', 'dealprice', 'amp']]
                result_temp[['open', 'close', 'high', 'low', 'deal', 'dealprice']] = result_temp[['open', 'close', 'high', 'low', 'deal', 'dealprice']].apply(pd.to_numeric)
                df_temp = df_temp.append(result_temp)
                #result_temp.to_sql(name=db_name, con=conn, if_exists='append', index=False)
            except Exception as exc:
                print('error on '+stock_id[i]+' '+db_name)
                print(exc)
                err_stock_id.append(stock_id[i])
                err_stock_id_for_url.append(stock_id_for_url[i])
                continue
            break
        #sys.stdout.write("\r{0}".format((float(i)/len(stock_id_for_url))*100))
        #sys.stdout.flush()
    df_temp.to_sql(name=db_name, con=conn, if_exists='append', index=False)
    return err_stock_id,err_stock_id_for_url

