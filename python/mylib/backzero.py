import pandas as pd
import mylib.macd

def get_MACD_DIF(df,short=12,long=26,M=9):
    ndf=df.tail(200)
    a=mylib.macd.get_EMA(df,short)
    b=mylib.macd.get_EMA(df,long)
    ma=(ndf.high.sum()-ndf.low.sum())/200
    df['diff']=(pd.Series(a)-pd.Series(b))/ma
    #print(df['diff'])
    '''
    for i in range(len(df)):
        if i==0:
            df.loc[i,'dea']=df.loc[i,'diff']
        if i>0:
            df.loc[i,'dea']=((M-1)*df.loc[i-1,'dea']+2*df.loc[i,'diff'])/(M+1)
    df['macd']=2*(df['diff']-df['dea'])
    '''
    return df

def zerodif (df,p):
    #print(df)
    ndf=df.tail(1)
    dif_value = ndf['diff'].values[0]
    if abs(dif_value)<p:
        stock_id = [(ndf['stock_id'].values[0])]
        date = [str(ndf['date'].values[0])]
        results = pd.DataFrame()
        results['stock_id'] = stock_id
        results['date'] = date
        return results
    else:
        pass
