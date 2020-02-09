#此脚本为日线二次金叉计算

import pandas as pd
from sqlalchemy import create_engine
import mylib.pachong
from tqdm import tqdm
from multiprocessing import Pool
from requests.adapters import HTTPAdapter
import requests,re,json,time,urllib
import pymysql
import mylib.macd
import mylib.gxfunctions
from config import *

#落入数据库
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format(usr_name, db_password, db_server, 'dfcf','utf8mb4'))
con = engine.connect()#创建连接

#创建本地数据库连接，并建立新的数据库
#建立本地数据库连接(需要先开启数据库服务)
#db = pymysql.connect(db_host, usr_name, db_password, charset='utf8mb4')
#cursor = db.cursor()

#提取所有代码，生成DataFrame存储URL
base_stocks = pd.read_sql('select * from dfcf.listtable',con)
target_stocks_ = pd.read_sql('select distinct stock_id from dfcf.daily_result',con)
selected_stocks = pd.merge(target_stocks_, base_stocks, on=['stock_id'], how='inner')

stock_id = list(selected_stocks['stock_id'])
stock_id_for_url = list(selected_stocks['stock_id_dfcf'])
stock_name = list(selected_stocks['名称'])

url_head_k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=k&style=top&num=540&rtntype=1&id='
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

def update_macdx(df_macdx):
    #df_temp_i = df_temp[df_temp['stock_id']==stock_id].reset_index(drop=True)
    if len(df_macdx) > 40:#上市40天的股票才考虑？
        df_temp_macd = mylib.macd.get_MACD(df_macdx)#要求df_temp_i至少有40个数据
        #print(df_temp_macd)
        len_t = len(df_temp_macd)-40
        df_temp_macd_t = df_temp_macd.tail(len_t).reset_index(drop=True)  # 要求df_temp_i至少有40个数据
        df_temp_macd_t.drop(['name','volume','dealprice','amp'],axis=1,inplace=True)
        #df_temp_macd_t.to_sql(name='daily_macd', con=con, if_exists='append', index=False)
        df_bottom_date=mylib.gxfunctions.goldx_now(df_temp_macd.tail(40))
        if df_bottom_date is not None:
            df_bottom_date.to_sql(name='daily_secondgx', con=con, if_exists='append', index=False)
            #df_bottom_date_hist =mylib.gxfunctions.goldx(df_temp_macd_t)
            #df_bottom_date_hist.to_sql(name='df_bottom_date_hist', con=con, if_exists='append', index=False)
            #print(daily_bottom)
        else:
            pass
    else:
        pass

#生成历史数据函数，并保存入mysql数据库
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
            columns = {0:"date",1:"open",2:"close",3:"high",4:"low",5:"volume",6:"dealprice",7:"amp",8:"turnover"}
            df_db.rename(columns = columns,inplace=True)
            stock_id_t = list(stock_info_init.loc[stock_info_init[url_type]==url,'stock_id'])[0]
            df_db['stock_id'] = stock_id_t
            df_db['name'] =list(stock_info_init.loc[stock_info_init[url_type]==url,'name'])[0]
            df_db = df_db[['name', 'stock_id', 'date', 'open', 'close', 'high', 'low', 'volume', 'dealprice', 'amp','turnover']]
            #print(df_db.tail(1))
            df_db[['open', 'close', 'high', 'low', 'volume','turnover']] = df_db[['open', 'close', 'high', 'low', 'volume','turnover']].apply(pd.to_numeric)
            #df_db.to_sql(name=db_name, con=con, if_exists='append', index=False)
            update_macdx(df_db)

        else:
            pass

    except Exception as exc:
        print('error on '+list(stock_info_init.loc[stock_info_init[url_type]==url,'stock_id'])[0]+' '+db_name)
        print(exc)

def mp_job(url_k):
    getHtml_dfcf(url_k)

def multicore(url_k_type):
    with Pool(5) as p:
        #p.map(job2, url_list)

        for _ in tqdm(p.imap_unordered(mp_job, url_k_type), total=len(url_k_type),ascii=True):
            pass

if __name__ == '__main__':

    #drop_exist_table = 'DROP TABLE IF EXISTS dfcf.dynamicdaily;'
    #cursor.execute(drop_exist_table)
    multicore(url_list_k)
    print('日k线实时二次金叉计算完成')
