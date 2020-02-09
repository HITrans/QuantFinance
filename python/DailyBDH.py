import pymysql
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from tqdm import tqdm
import mylib.sql_lib
import mylib.gxfunctions
import mylib.macd
from multiprocessing import Pool

dbpasswords='1qaz2wsx'
con = pymysql.connect(host='127.0.0.1', user='root', passwd=dbpasswords, db='dfcf', port = 3306) # 连接
cur = con.cursor()

#获取股票代码列表
stock_id_list_sql = 'select distinct 代码 from dfcf.stock_basic_info'
df_stockid = mylib.sql_lib.read_table(cur,stock_id_list_sql)
df_stockid.columns = ['stock_id']
stock_id_list=df_stockid['stock_id'].tolist()

#获取更新后的计算日线数据库
sql_t_nd = 'select stock_id,date,open,close,high,low from dfcf.hist_daily_data'
df_temp = mylib.sql_lib.read_table(cur, sql_t_nd)
df_temp.columns = ['stock_id','date','open','close','high','low']
df_temp[['open','close','high','low']] = df_temp[['open','close','high','low']].apply(pd.to_numeric)

#初始化数据库
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', dbpasswords, '127.0.0.1:3306', 'dfcf','utf8'))
con = engine.connect()#创建连接



def main_macd (stock_id):
    df_temp_i = df_temp[df_temp['stock_id']==stock_id].reset_index(drop=True)
    if len(df_temp_i.stock_id) > 40:#上市40天的股票才考虑？
        df_temp_macd = mylib.macd.get_MACD(df_temp_i).tail(500).reset_index(drop=True)#要求df_temp_i至少有700个数据
        df_temp_macd.to_sql(name='daily_macd', con=con, if_exists='append', index=False)
        df_bottom_date=mylib.gxfunctions.goldx(df_temp_macd)
        df_bottom_date.to_sql(name='daily_xdate', con=con, if_exists='append', index=False)
    else:
        pass

def mp_job(stock_id):
    main_macd(stock_id)

def multicore():
    with Pool(5) as p:
        #p.map(job2, url_list)

        for _ in tqdm(p.imap_unordered(mp_job, stock_id_list), total=len(stock_id_list)):
            pass

if __name__ == '__main__':
    multicore()
    print('done')
