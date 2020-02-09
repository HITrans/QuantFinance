# -*- coding: utf-8 -*-
#此脚本为初始化程序，初始化应用时执行一次

import pandas as pd
from sqlalchemy import create_engine
import mylib.pachong
from tqdm import tqdm
from multiprocessing import Pool
from requests.adapters import HTTPAdapter
import requests,re,json,time,urllib
from config import *


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

#提取所有代码，生成DataFrame存储URL
stock_id = list(main_table['代码'])
stock_id_for_url = list(main_table['mid'])
stock_name = list(main_table['名称'])

url_head_k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=k&style=top&num=700&rtntype=1&id='
url_list_k = [url_head_k+stock_ids for stock_ids in stock_id_for_url]

url_head_m5k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=m5k&style=top&num=5000&rtntype=1&id='
url_list_m5k = [url_head_m5k+stock_ids for stock_ids in stock_id_for_url]

url_head_m15k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=m15k&style=top&num=5000&rtntype=1&id='
url_list_m15k = [url_head_m15k+stock_ids for stock_ids in stock_id_for_url]

url_head_m30k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=m30k&style=top&num=5000&rtntype=1&id='
url_list_m30k = [url_head_m30k+stock_ids for stock_ids in stock_id_for_url]

url_head_m60k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=m60k&style=top&num=5000&rtntype=1&id='
url_list_m60k = [url_head_m60k+stock_ids for stock_ids in stock_id_for_url]

url_head_wk = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=wk&style=top&num=270&rtntype=1&id='
url_list_wk = [url_head_wk+stock_ids for stock_ids in stock_id_for_url]

url_list_total = [url_list_k,url_list_m5k,url_list_m15k,url_list_m30k,url_list_m60k,url_list_wk]

stock_info_init = pd.DataFrame()
stock_info_init['name'] = stock_name
stock_info_init['stock_id'] = stock_id
stock_info_init['url_k'] = url_list_k
stock_info_init['url_m5k'] = url_list_m5k
stock_info_init['url_m15k'] = url_list_m15k
stock_info_init['url_m30k'] = url_list_m30k
stock_info_init['url_m60k'] = url_list_m60k
stock_info_init['url_wk'] = url_list_wk

#落入数据库
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format(usr_name, db_password, db_server, 'dfcf','utf8mb4'))
con = engine.connect()#创建连接

def getHtml_dfcf(url):
    #url = 'http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cb=jQuery112403755036159032368_1561627712037&type=CT&token=4f1862fc3b5e77c150a2b985b12db0fd&sty=FCOIATC&js=(%7Bdata%3A%5B(x)%5D%2CrecordsFiltered%3A(tot)%7D)&cmd=C._A&st=(ChangePercent)&sr=-1&p=1&ps=5000&_=1561627712203'
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=40))
    s.mount('https://', HTTPAdapter(max_retries=40))
    #print(time.strftime('%Y-%m-%d %H:%M:%S'))
    try:

        r = s.get(url,timeout=20)
        content = r.text
        content = content.replace('jQuery183(', '').replace(')', '').strip()
        content = content.split('\r\n')
        content = [x.split(',') for x in content]
        get_hist_daily_mp_dfcf(content,url)
        #return content
    except requests.exceptions.RequestException as e:
        print(e)
        print(time.strftime('%Y-%m-%d %H:%M:%S'))


#生成历史数据函数，并保存入mysql数据库
def get_hist_daily_mp_dfcf(result_temp,url):
    if 'm5k' in url:
        url_type = 'url_m5k'
        db_name = 'hist_m5k_data'
    elif '=k' in url:
        url_type = 'url_k'
        db_name = 'hist_daily_data'
    elif 'm15k' in url:
        url_type = 'url_m15k'
        db_name = 'hist_m15k_data'
    elif 'm30k' in url:
        url_type = 'url_m30k'
        db_name = 'hist_m30k_data'
    elif 'm60k' in url:
        url_type = 'url_m60k'
        db_name = 'hist_m60k_data'
    elif 'wk' in url:
        url_type = 'url_wk'
        db_name = 'hist_wk_data'
    try:
        if result_temp[0][0] != '{stats:false}':
            df_db = pd.DataFrame(result_temp)
            columns = {0:"date",1:"open",2:"close",3:"high",4:"low",5:"volume",6:"dealprice",7:"amp",8:"turnover"}
            df_db.rename(columns = columns,inplace=True)
            stock_id_t = list(stock_info_init.loc[stock_info_init[url_type]==url,'stock_id'])[0]
            df_db['stock_id'] = stock_id_t
            df_db['name'] =list(stock_info_init.loc[stock_info_init[url_type]==url,'name'])[0]
            df_db_1 = df_db[['name', 'stock_id', 'date', 'open', 'close', 'high', 'low', 'volume', 'dealprice', 'amp']]
            df_db_1[['open', 'close', 'high', 'low', 'volume']] = df_db_1[['open', 'close', 'high', 'low', 'volume']].apply(pd.to_numeric)
            df_db_1.to_sql(name=db_name, con=con, if_exists='append', index=False)
            if '=k' in url:
                df_db_2 = df_db[['name', 'stock_id', 'date', 'turnover']]
                df_db_2.to_sql(name='k_turnover_hist', con=con, if_exists='append', index=False)
            else:
                pass
        else:
            pass

    except Exception as exc:
        print('error on '+list(stock_info_init.loc[stock_info_init[url_type]==url,'stock_id'])[0]+' '+db_name)
        print(exc)

def mp_job(url_k):
    getHtml_dfcf(url_k)

def multicore(url_k_type):
    with Pool(30) as p:
        #p.map(job2, url_list)

        for _ in tqdm(p.imap_unordered(mp_job, url_k_type), total=len(url_k_type),ascii=True):
            pass

if __name__ == '__main__':

    start = time.time()
    multicore(url_list_k)
    print('日k线历史数据下载完成')
    multicore(url_list_m5k)
    print('5分钟k线历史数据下载完成')
    multicore(url_list_m15k)
    print('15分钟k线历史数据下载完成')
    multicore(url_list_m30k)
    print('30分钟k线历史数据下载完成')
    multicore(url_list_m60k)
    print('60分钟k线历史数据下载完成')
    multicore(url_list_wk)
    print('周k线历史数据下载完成')
    end = time.time()
    print(end - start)
