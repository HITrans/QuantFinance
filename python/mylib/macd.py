import pandas as pd



# 指标计算函数
def get_EMA(df, N):
    for i in range(len(df)):
        if i == 0:
            df.loc[i, 'ema'] = df.loc[i, 'close']
        #            df.ix[i,'ema']=0
        if i > 0:
            df.loc[i, 'ema'] = (2 * df.loc[i, 'close'] + (N - 1) * df.loc[i - 1, 'ema']) / (N + 1)
    ema = list(df['ema'])
    return ema


def get_MACD(df, short=12, long=26, M=9):
    a = get_EMA(df, short)
    b = get_EMA(df, long)
    df['diff'] = pd.Series(a) - pd.Series(b)
    # print(df['diff'])
    for i in range(len(df)):
        if i == 0:
            df.loc[i, 'dea'] = df.loc[i, 'diff']
        if i > 0:
            df.loc[i, 'dea'] = ((M - 1) * df.loc[i - 1, 'dea'] + 2 * df.loc[i, 'diff']) / (M + 1)
    df['macd'] = 2 * (df['diff'] - df['dea'])
    return df