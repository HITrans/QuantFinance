import numpy as np
import pandas as pd

def goldx_now(ndf):  # 新入数据需要先看是否金叉，从指标库里选出最新50条数据，即ndf=df_temp.tail(50)
    # global bottom_deathx_tstamp_now
    ddf = ndf.tail(2).reset_index(drop=True)
    ndf = ndf.tail(100).reset_index(drop=True)
    if ddf.loc[0, 'diff'] < ddf.loc[0, 'dea'] and ddf.loc[1, 'diff'] > ddf.loc[1, 'dea'] and ddf.loc[1, 'diff'] < 0:
#判断当下是否金叉，如果金叉，记录金叉时刻
        bottom_goldx_tstamp_now = ddf[['date']].loc[1].reset_index(drop=True)
        index_list = ndf.index.values.tolist()

        for k in range(1, len(ndf) - 1):
            if ndf.loc[index_list[-(k + 1)], 'diff'] > ndf.loc[index_list[-(k + 1)], 'dea'] and ndf.loc[
                index_list[-k], 'diff'] < ndf.loc[index_list[-k], 'dea'] and ndf.loc[index_list[-k], 'diff'] < 0:
                continue
            else:
                pass
            bottom_deathx_tstamp_now = ndf[['date']].loc[index_list[-k]].reset_index(drop=True)
            break

        new_bottom_x = pd.concat([bottom_deathx_tstamp_now, bottom_goldx_tstamp_now], axis=1)
        new_bottom_x.columns = ['death_date', 'gold_date']
        stock_id = ndf.stock_id[0]
        new_bottom_x['stock_id'] = stock_id
        return new_bottom_x
    else:
        return None


def goldx(ndf):  # dataframe必须来自指标库，且包含时间信息

    deathx_list = []
    goldx_list = []

    for i in range(1, len(ndf)):
        if ndf.loc[i - 1, 'diff'] < ndf.loc[i - 1, 'dea'] and ndf.loc[i, 'diff'] > ndf.loc[i, 'dea']:
            goldx_list.append(i)
        if ndf.loc[i - 1, 'diff'] > ndf.loc[i - 1, 'dea'] and ndf.loc[i, 'diff'] < ndf.loc[i, 'dea']:
            deathx_list.append(i)
        else:
            pass

    bottom_deathx_list = []
    bottom_goldx_list = []
    if len(deathx_list)>0 and len(goldx_list)>0:
        if len(deathx_list) == len(goldx_list):
            if deathx_list[-1] < goldx_list[-1]:
                for i in range(1, len(goldx_list)):
                    gold_diff_temp = ndf.loc[goldx_list[-i], ['diff']].values[0]
                    gold_dea_temp = ndf.loc[goldx_list[-i], ['dea']].values[0]
                    death_diff_temp = ndf.loc[deathx_list[-i], ['diff']].values[0]
                    death_dea_temp = ndf.loc[deathx_list[-i], ['dea']].values[0]
                    if gold_diff_temp < 0 and gold_dea_temp < 0 and death_diff_temp < 0 and death_dea_temp < 0:
                        bottom_deathx_list.append(deathx_list[-i])
                        bottom_goldx_list.append(goldx_list[-i])
                    else:
                        pass
            if deathx_list[-1] > goldx_list[-1]:
                for i in range(1, len(goldx_list) - 1):
                    gold_diff_temp = ndf.loc[goldx_list[-i], ['diff']].values[0]
                    gold_dea_temp = ndf.loc[goldx_list[-i], ['dea']].values[0]
                    death_diff_temp = ndf.loc[deathx_list[-(i + 1)], ['diff']].values[0]
                    death_dea_temp = ndf.loc[deathx_list[-(i + 1)], ['dea']].values[0]
                    if gold_diff_temp < 0 and gold_dea_temp < 0 and death_diff_temp < 0 and death_dea_temp < 0:
                        bottom_deathx_list.append(deathx_list[-(i + 1)])
                        bottom_goldx_list.append(goldx_list[-i])
                    else:
                        pass
            else:
                pass

        if len(deathx_list) > len(goldx_list):
            for i in range(1, len(goldx_list)):
                gold_diff_temp = ndf.loc[goldx_list[-i], ['diff']].values[0]
                gold_dea_temp = ndf.loc[goldx_list[-i], ['dea']].values[0]
                death_diff_temp = ndf.loc[deathx_list[-(i + 1)], ['diff']].values[0]
                death_dea_temp = ndf.loc[deathx_list[-(i + 1)], ['dea']].values[0]
                if gold_diff_temp < 0 and gold_dea_temp < 0 and death_diff_temp < 0 and death_dea_temp < 0:
                    bottom_deathx_list.append(deathx_list[-(i + 1)])
                    bottom_goldx_list.append(goldx_list[-i])
                else:
                    pass

        if len(deathx_list) < len(goldx_list):
            for i in range(1, len(deathx_list)):
                gold_diff_temp = ndf.loc[goldx_list[-i], ['diff']].values[0]
                gold_dea_temp = ndf.loc[goldx_list[-i], ['dea']].values[0]
                death_diff_temp = ndf.loc[deathx_list[-i], ['diff']].values[0]
                death_dea_temp = ndf.loc[deathx_list[-i], ['dea']].values[0]
                # print(death_diff_temp,gold_diff_temp)
                if gold_diff_temp < 0 and gold_dea_temp < 0 and death_diff_temp < 0 and death_dea_temp < 0:
                    bottom_deathx_list.append(deathx_list[-i])
                    bottom_goldx_list.append(goldx_list[-i])
                else:
                    pass
        else:
            pass
    else:
        pass

    bottom_deathx_tstamp = ndf[['date']].loc[bottom_deathx_list].reset_index(drop=True)
    bottom_deathx_tstamp.columns = ['death_date']
    bottom_goldx_tstamp = ndf[['date']].loc[bottom_goldx_list].reset_index(drop=True)
    bottom_goldx_tstamp.columns = ['gold_date']
    bottom_divergencex_tstamp = pd.concat([bottom_deathx_tstamp, bottom_goldx_tstamp], axis=1)
    stock_id = ndf.stock_id[0]

    bottom_divergencex_tstamp['stock_id'] = stock_id

    return bottom_divergencex_tstamp


# 计算底背离

def BottomDivergence(df, n, deathx_list, goldx_list):
    #print(df)
    if goldx_list:

        low_value_0 = \
        df.loc[deathx_list[0]:goldx_list[0], ['low']].sort_values(['low'], ascending=True).head(1).values[0][0]
        diff_value_0 = \
        df.loc[deathx_list[0]:goldx_list[0], ['diff']].sort_values(['diff'], ascending=True).head(1).values[0][0]
        #print(low_value_0)


        if n <= len(goldx_list):
            low_value_l = []
            diff_value_l = []
            for i in range(1, n):
                # print(i)
                low_value = \
                df.loc[deathx_list[i]:goldx_list[i], ['low']].sort_values(['low'], ascending=True).head(1).values[0][0]
                diff_value = \
                df.loc[deathx_list[i]:goldx_list[i], ['diff']].sort_values(['diff'], ascending=True).head(1).values[0][
                    0]
                low_value_l.append(low_value)
                diff_value_l.append(diff_value)
                #print(low_value,diff_value)
            min_low_value = min(low_value_l)
            diff_value = diff_value_l[low_value_l.index(min_low_value)]
            #print(low_value_l)
            #print(diff_value_l)
            #print("low_value_0: "+low_value_0)
            #print("diff_value_0: "+ diff_value_0)

            if low_value_0 <= min_low_value and diff_value_0 > diff_value:
                print(df.stock_id[0])
                print(low_value_l)
                print(diff_value_l)
                print("low_value_0: "+str(low_value_0))
                print("diff_value_0: "+ str(diff_value_0))
                result_df = df[['stock_id','date']].tail(1).reset_index(drop=True)
                return result_df
                #break
            else:
                pass
        else:
            low_value_l = []
            diff_value_l = []
            for i in range(1, len(goldx_list)):
                low_value = \
                df.loc[deathx_list[i]:goldx_list[i], ['low']].sort_values(['low'], ascending=True).head(1).values[0][0]
                diff_value = \
                df.loc[deathx_list[i]:goldx_list[i], ['diff']].sort_values(['diff'], ascending=True).head(1).values[0][
                    0]
                low_value_l.append(low_value)
                diff_value_l.append(diff_value)
            min_low_value = min(low_value_l)
            diff_value = diff_value_l[low_value_l.index(min_low_value)]
            #print(low_value_l)
            #print(diff_value_l)
            #print("low_value_0: "+low_value_0)
            #print("diff_value_0: "+ diff_value_0)

            if low_value_0 <= min_low_value and diff_value_0 > diff_value:
                print(df.stock_id[0])
                print(low_value_l)
                print(diff_value_l)
                print("low_value_0: "+str(low_value_0))
                print("diff_value_0: "+ str(diff_value_0))
                result_df = df[['stock_id','date']].tail(1).reset_index(drop=True)
                return result_df
                #break
            else:
                pass
            # print('find it!low value is %f and diff value is %f' %(low_value,diff_value))
    else:
        pass

