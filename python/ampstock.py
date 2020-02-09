'''
放量计算函数，读入一只股票的dataframe，然后计算出是否满足条件后pass或者给出股票id和对应日期
df至少保证有stock_id，date和rate（换手率）两个量，每只股票保存50天（日线）换手率，其他规则同底背离计算
计算结果仍然是append进入放量结果库，前端不删除，后台不管
'''

def ampstock (df):
    
    ndf5=df.tail(5).reset_index(drop=True)
    ndf10=df.tail(10).reset_index(drop=True)
    ndf20=df.tail(20).reset_index(drop=True)
    ndf30=df.tail(30).reset_index(drop=True)
    
    tun5=2*ndf5.rate.mean()
    tun10=2*ndf10.rate.mean()
    tun20=2*ndf20.rate.mean()
    tun30=2*ndf30.rate.mean()
    
    tun=df.tail(1).rate.values[0]
    result_df = df[['stock_id','date']].tail(1).reset_index(drop=True)
    if tun>=tun5 or tun>=tun10 or tun>=tun20 or tun>=tun30:
        return result_df
    else:
        #print('shit')
        pass