import pandas as pd
#import os
#import requests,re,json,time,urllib
#import heapq
#from bs4 import BeautifulSoup
#import sys
import pymysql
from sqlalchemy import create_engine
import mylib.pachong,mylib.sql_lib,mylib.others
import tushare as ts
import datetime
from tqdm import tqdm


#创建本地数据库连接
con = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='dfcf', port = 3306) # 连接
cur = con.cursor()

#更新各种大表
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
stock_id_for_url = list(main_table['mid'])


#Tushare取出开盘日列表
#alldays = ts.trade_cal()
#tradingdays = alldays[alldays['isOpen'] == 1]   # 开盘日

#生成历史数据函数，并保存入mysql数据库
def get_hist_daily(stock_id_for_url,url,db_name):
    err_stock_id = []
    err_stock_id_for_url = []
    for i in tqdm(range(len(stock_id_for_url))):
        while True:
            try:
                url_t = url+stock_id_for_url[i]
                result_temp = mylib.pachong.get_stock_hist(url_t)
                result_temp['stock_id'] = str(stock_id[i])
                result_temp['name'] =list(main_table.loc[main_table['mid']==stock_id_for_url[i],'名称'])[0]
                if len(result_temp['stock_id'])==0:
                    result_temp = pd.DataFrame(0, index=[0],columns=['name', 'stock_id', 'date', 'open', 'close', 'high', 'low', 'deal', 'dealprice', 'amp'])
                    result_temp['stock_id'] = str(stock_id[i])
                    result_temp['name'] =list(main_table.loc[main_table['mid']==stock_id_for_url[i],'名称'])[0]
                else:
                    pass
                result_temp = result_temp[['name', 'stock_id', 'date', 'open', 'close', 'high', 'low', 'deal', 'dealprice', 'amp']]
                result_temp[['open', 'close', 'high', 'low', 'deal', 'dealprice']] = result_temp[['open', 'close', 'high', 'low', 'deal', 'dealprice']].apply(pd.to_numeric)
                result_temp.to_sql(name=db_name, con=con, if_exists='append', index=False)
            except Exception as exc:
                print('error on '+stock_id[i]+' '+db_name)
                print(exc)
                err_stock_id.append(stock_id[i])
                err_stock_id_for_url.append(stock_id_for_url[i])
                continue
            break
        #sys.stdout.write("\r{0}".format((float(i)/len(stock_id_for_url))*100))
        #sys.stdout.flush()
    return err_stock_id,err_stock_id_for_url

#----------------------------------------------------------日线数据断点更新-----------------------------------------------
sql_t = 'select 时间 from hist_daily_data where 代码=601988'
df_temp = mylib.sql_lib.read_table(cur, sql_t)
last_day_kt = list(df_temp.iloc[:,0])[-1]
data_type = 'k'
updating_days_count = mylib.others.get_update_counts(last_day_kt,data_type )
print('日线需更新天数= '+str(updating_days_count))
url_k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=k&style=top&num='+str(updating_days_count)+'&rtntype=6&id='
db_n = 'hist_daily_data'
if updating_days_count == 0:
    print('日线数据不需要进行断点更新')
else:
    print('开始进行日数据断点更新')
    get_hist_daily(stock_id_for_url,url_k,db_n)
print('日K线断点更新完成')

#-------------------------------------------------------------m5k数据断点更新---------------------------------------------
sql_t = 'select 时间 from hist_m5k_data where 代码=601988'
df_temp = mylib.sql_lib.read_table(cur, sql_t)
last_day_5t = list(df_temp.iloc[:,0])[-1]
data_type = 'm5k'
updating_counts_m5k = mylib.others.get_update_counts(last_day_5t,data_type )
print('m5k数据需更新条数= '+str(updating_counts_m5k))
url_m5k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=m5k&style=top&num='+str(updating_counts_m5k)+'&rtntype=6&id='
db_n = 'hist_m5k_data'
if updating_counts_m5k == 0:
    print('5分钟k线数据不需要进行断点更新')
else:
    print('开始进行5分钟k数据断点更新')
    get_hist_daily(stock_id_for_url,url_m5k,db_n)
print('m5K线断点更新完成')
'''
#-------------------------------------------------------------m15k数据断点更新---------------------------------------------
sql_t = 'select 时间 from hist_m15k_data where 代码=601988'
df_temp = mylib.sql_lib.read_table(cur, sql_t)
last_day_15t = list(df_temp.iloc[:,0])[-1]
data_type = 'm15k'
updating_counts_m15k = mylib.others.get_update_counts(last_day_15t,data_type )
print('m15k数据需更新条数= '+str(updating_counts_m15k))
url_m15k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=m15k&style=top&num='+str(updating_counts_m15k)+'&rtntype=6&id='
db_n = 'hist_m15k_data'
if updating_counts_m15k == 0:
    print('15分钟k线数据不需要进行断点更新')
else:
    print('开始进行15分钟k数据断点更新')
    get_hist_daily(stock_id_for_url,url_m15k,db_n)
print('m15K线断点更新完成')

#-------------------------------------------------------------m30k数据断点更新---------------------------------------------
sql_t = 'select 时间 from hist_m30k_data where 代码=601988'
df_temp = mylib.sql_lib.read_table(cur, sql_t)
last_day_30t = list(df_temp.iloc[:,0])[-1]
data_type = 'm30k'
updating_counts_m30k = mylib.others.get_update_counts(last_day_30t,data_type )
print('m30k数据需更新条数= '+str(updating_counts_m30k))
url_m30k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=m30k&style=top&num='+str(updating_counts_m30k)+'&rtntype=6&id='
db_n = 'hist_m30k_data'
if updating_counts_m30k == 0:
    print('30分钟k线数据不需要进行断点更新')
else:
    print('开始进行30分钟k数据断点更新')
    get_hist_daily(stock_id_for_url,url_m30k,db_n)
print('m30K线断点更新完成')

#-------------------------------------------------------------m60k数据断点更新---------------------------------------------
sql_t = 'select 时间 from hist_m60k_data where 代码=601988'
df_temp = mylib.sql_lib.read_table(cur, sql_t)
last_day_60t = list(df_temp.iloc[:,0])[-1]
data_type = 'm60k'
updating_counts_m60k = mylib.others.get_update_counts(last_day_60t,data_type )
print('m60k数据需更新条数= '+str(updating_counts_m60k))
url_m60k = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=m60k&style=top&num='+str(updating_counts_m60k)+'&rtntype=6&id='
db_n = 'hist_m60k_data'
if updating_counts_m60k == 0:
    print('60分钟k线数据不需要进行断点更新')
else:
    print('开始进行60分钟k数据断点更新')
    get_hist_daily(stock_id_for_url,url_m60k,db_n)
print('m60K线断点更新完成')'''

