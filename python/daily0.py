import mylib.backzero
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import numpy as np
from tqdm import tqdm
import mylib.sql_lib
from config import *
from requests.adapters import HTTPAdapter
import requests,re,json,time,urllib
from multiprocessing import Pool

con = pymysql.connect(host='47.96.28.113', user='root' , passwd='1qaz@WSX', db='dfcf', port = 42036,charset='utf8mb4') # 连接
cur = con.cursor()

#落入数据库
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format(usr_name, db_password, db_server, 'dfcf','utf8mb4'))
con = engine.connect()#创建连接

#提取所有代码，生成DataFrame存储URL
selected_stocks = pd.read_sql('select * from dfcf.listtable',con)


sql1 = 'select p_read from dfcf.parameter where id = 1;'
cur.execute(sql1)
para_p = float(cur.fetchone()[0])

stock_id = list(selected_stocks['stock_id'])
stock_id_for_url = list(selected_stocks['stock_id_dfcf'])
stock_name = list(selected_stocks['名称'])

url_head_k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=k&style=top&num=40&rtntype=1&id='
url_list_k = [url_head_k+stock_ids for stock_ids in stock_id_for_url]

stock_info_init = pd.DataFrame()
stock_info_init['name'] = stock_name
stock_info_init['stock_id'] = stock_id
stock_info_init['url_k'] = url_list_k

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

def get_hist_daily_mp_dfcf(result_temp,url):
    if 'm5k' in url:
        url_type = 'url_m5k'
        db_name = 'hist_m5k_data'
    elif '=k' in url:
        url_type = 'url_k'
        db_name = 'dynamicdaily'
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
            columns = {0:"date",1:"open",2:"close",3:"high",4:"low",5:"volume",6:"dealprice",7:"amp"}
            df_db.rename(columns = columns,inplace=True)
            stock_id_t = list(stock_info_init.loc[stock_info_init[url_type]==url,'stock_id'])[0]
            df_db['stock_id'] = stock_id_t
            df_db['name'] =list(stock_info_init.loc[stock_info_init[url_type]==url,'name'])[0]
            df_db = df_db[['name', 'stock_id', 'date', 'open', 'close', 'high', 'low', 'volume', 'dealprice', 'amp']]
            df_db[['open', 'close', 'high', 'low']] = df_db[['open', 'close', 'high', 'low']].apply(pd.to_numeric)
            #df_db.to_sql(name=db_name, con=con, if_exists='append', index=False)
            draw_back(df_db,para_p)
        else:
            pass
    except Exception as exc:
        print('error on '+list(stock_info_init.loc[stock_info_init[url_type]==url,'stock_id'])[0]+' '+'draw_back_result')
        print(exc)

def draw_back(df_temp,para):
    #result_stock = pd.DataFrame()
    p = para
    if len(df_temp)>=40:
        df_dif=mylib.backzero.get_MACD_DIF(df_temp)
        if df_dif is not None:
            result = mylib.backzero.zerodif(df_dif, p)
            if result is not None:
                #print(result)
                #result_stock['stock_id'] = [str(result)]
                result.to_sql(name='draw_back_result', con=con, if_exists='append', index=False)
            else:
                pass
        else:
            pass
    else:
        pass

def mp_job(url_k):
    getHtml_dfcf(url_k)

def multicore(url_k_type):
    with Pool(10) as p:
        #p.map(job2, url_list)

        for _ in tqdm(p.imap_unordered(mp_job, url_k_type), total=len(url_k_type),ascii=True):
            pass

if __name__ == '__main__':

    multicore(url_list_k)
    print('日线回抽计算完成')