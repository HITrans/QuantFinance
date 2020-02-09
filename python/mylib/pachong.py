#此文件主要负责基础爬虫程序
import requests,re,json,time,urllib
import pandas as pd
import time
from requests.adapters import HTTPAdapter
#from fake_useragent import UserAgent

#ua = UserAgent(use_cache_server=False)

def getHtml(url):
    #url = 'http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cb=jQuery112403755036159032368_1561627712037&type=CT&token=4f1862fc3b5e77c150a2b985b12db0fd&sty=FCOIATC&js=(%7Bdata%3A%5B(x)%5D%2CrecordsFiltered%3A(tot)%7D)&cmd=C._A&st=(ChangePercent)&sr=-1&p=1&ps=5000&_=1561627712203'
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=40))
    s.mount('https://', HTTPAdapter(max_retries=40))
    print(time.strftime('%Y-%m-%d %H:%M:%S'))
    try:
        #ua = UserAgent(use_cache_server=False)
        #headers = {'User-Agent':ua.random}
        r = s.get(url)
        pat = "data:\[(.*?)\]"
        data = re.compile(pat,re.S).findall(r.text)
        return data
    except requests.exceptions.RequestException as e:
        print(e)
        print(time.strftime('%Y-%m-%d %H:%M:%S'))

def getStock(url):
    data = getHtml(url)
    datas = data[0].split('","')
    stocks = []
    for i in range(len(datas)):
        stock = datas[i].replace('"',"").split(",")
        stocks.append(stock)
    return stocks

#生成历史K线数据，每次调取生成一直股票的，通过改url中的id来生成所有股票的数据和生成天数
def get_stock_hist(url):
    #sample url
    #url = 'http://pdfm.eastmoney.com/em_ubg_pdti_fast/api/js?cb=jQuery183&TYPE=k&style=top&num=500&rtntype=6&id=0006072'
    pages = requests.get(url)
    texts = pages.text.split('(')[1].split(')')[0]
    stock = json.loads(texts)
    data = stock['data']
    stocks_h = []
    for i in range(len(data)):
        stock = data[i].replace('"',"").split(",")
        stocks_h.append(stock)
    df_t = pd.DataFrame(stocks_h)
    #df_t['id'] = '000607'
    columns = {0:"date",1:"open",2:"close",3:"high",4:"low",5:"deal",6:"dealprice",7:"amp"}
    df_t.rename(columns = columns,inplace=True)
    return df_t

def getHtml_dfcf(url):
    #url = 'http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cb=jQuery112403755036159032368_1561627712037&type=CT&token=4f1862fc3b5e77c150a2b985b12db0fd&sty=FCOIATC&js=(%7Bdata%3A%5B(x)%5D%2CrecordsFiltered%3A(tot)%7D)&cmd=C._A&st=(ChangePercent)&sr=-1&p=1&ps=5000&_=1561627712203'
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=40))
    s.mount('https://', HTTPAdapter(max_retries=40))
    #print(time.strftime('%Y-%m-%d %H:%M:%S'))
    try:
        ua = UserAgent(use_cache_server=False)
        headers = {'User-Agent':ua.random}
        r = s.get(url,headers = headers)
        content = r.text
        content = content.replace('jQuery183(', '').replace(')', '').strip()
        content = content.split('\r\n')
        content = [x.split(',') for x in content]
        get_hist_daily_mp_dfcf(content,url,db_name)
        #return content
    except requests.exceptions.RequestException as e:
        print(e)
        print(time.strftime('%Y-%m-%d %H:%M:%S'))


#生成历史数据函数，并保存入mysql数据库
def get_hist_daily_mp_dfcf(result_temp,url,db_name):
    try:
        if result_temp[0][0] != '{stats:false}':
            df_db = pd.DataFrame(result_temp)
            df_db['stock_id'] = list(stock_info_init.loc[stock_info_init['url_k']==url,'stock_id'])[0]
            df_db['name'] =list(stock_info_init.loc[stock_info_init['url_k']==url,'name'])[0]
        else:
            df_db = pd.DataFrame(0, index=[0],columns=['name', 'stock_id', 'date', 'open', 'close', 'high', 'low', 'volume', 'dealprice', 'amp'])
            df_db['stock_id'] = df_db['stock_id'] = list(stock_info_init.loc[stock_info_init['url_k']==url,'stock_id'])[0]
            df_db['name'] =list(stock_info_init.loc[stock_info_init['url_k']==url,'name'])[0]

        df_db = result_temp[['name', 'stock_id', 'date', 'open', 'close', 'high', 'low', 'volume', 'dealprice', 'amp']]
        df_db[['open', 'close', 'high', 'low', 'volume']] = result_temp[['open', 'close', 'high', 'low', 'volume']].apply(pd.to_numeric)
        df_db.to_sql(name=db_name, con=con, if_exists='append', index=True)
    except Exception as exc:
        print('error on '+list(stock_info_init.loc[stock_info_init['url_sina']==url,'stock_id'])[0]+' '+db_name)
        print(exc)


        #sys.stdout.write("\r{0}".format((float(i)/len(stock_id_for_url))*100))
        #sys.stdout.flush()
    #return err_stock_id,err_stock_id_for_url