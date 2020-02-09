import pymysql
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from tqdm import tqdm
import mylib.sql_lib
import mylib.gxfunctions
from config import *

def bdresult(db_name1,db_name2,db_name3,db_name4):

    con = pymysql.connect(host='127.0.0.1', user=usr_name , passwd=db_password, db='dfcf', port = 3306) # 连接
    cur = con.cursor()

    #获取股票代码列表
    stock_id_list_sql = 'select distinct 代码 from dfcf.stock_basic_info'
    df_stockid = mylib.sql_lib.read_table(cur,stock_id_list_sql)
    df_stockid.columns = ['stock_id']
    stock_id_list=df_stockid['stock_id'].tolist()

    #获取macd日线数据
    sql_t_macd = 'select stock_id,date,low,diff,dea from'+db_name1
    df_temp_macd = mylib.sql_lib.read_table(cur, sql_t_macd)
    df_temp_macd.columns=['stock_id', 'date', 'low',  'diff','dea']
    df_temp_macd[[ 'low', 'diff','dea']]=df_temp_macd[['low', 'diff','dea']].apply(pd.to_numeric)

    #获取历史金叉日程数据
    sql_t_xdate = 'select * from'+db_name2
    df_temp_xdate = mylib.sql_lib.read_table(cur, sql_t_xdate)
    df_temp_xdate.columns=['death_date', 'gold_date', 'stock_id']

    result = pd.DataFrame( columns=['stock_id','date'])

    for i in tqdm(stock_id_list):

        df_temp_macd_i = df_temp_macd[df_temp_macd['stock_id'] == i].reset_index(drop=True)
        df_temp_xdate_i = df_temp_xdate[df_temp_xdate['stock_id'] == i].reset_index(drop=True)

        if len(df_temp_macd_i.stock_id)>40:
            new_bottom_x_i = mylib.gxfunctions.goldx_now(df_temp_macd_i)

            if new_bottom_x_i is not None:
                deathx_list = [df_temp_macd_i[df_temp_macd_i['date'] == k].index.values[0].tolist() for k in
                               df_temp_xdate_i.death_date.tolist()]
                goldx_list = [df_temp_macd_i[df_temp_macd_i['date'] == k].index.values[0].tolist() for k in
                              df_temp_xdate_i.gold_date.tolist()]
                cal_result=mylib.gxfunctions.BottomDivergence(df_temp_macd_i,10, deathx_list, goldx_list)
                if cal_result is not None:
                    result=result.append(cal_result)
                else:
                    pass
            else:
                pass
        else:
            pass


    engine = create_engine(
        "mysql+pymysql://{}:{}@{}/{}?charset={}".format(usr_name, db_password, db_server, 'dfcf', 'utf8mb4'))
    con = engine.connect()  # 创建连接

    result.to_sql(name=db_name3, con=con, if_exists='replace', index=False) #写入动态调用库
    result.to_sql(name=db_name4, con=con, if_exists='append', index=False) # 写入历史数据库