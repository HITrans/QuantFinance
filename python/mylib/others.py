#其他用到的函数
import datetime
import calendar
import time
import tushare as ts

#Tushare取出开盘日列表
alldays = ts.trade_cal()
tradingdays = alldays[alldays['isOpen'] == 1]   # 开盘日

#生成两个日期间的日期列表
def getBetweenDay(begin_date):
    date_list = []
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(time.strftime('%Y-%m-%d',time.localtime(time.time())), "%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)
    return date_list



#计算更新区间有多少个交易日
def count_trading_days(days_list):
    count = 0
    for i in range(len(days_list)):
        if days_list[i] in tradingdays['calendarDate'].values:
            count = count+1
        else:
            pass
    return count-1


m5k_dict = {'09:35':0,'09:40':1,'09:45':2,'09:50':3,'09:55':4,'10:00':5,'10:05':6,'10:10':7,'10:15':8,'10:20':9,'10:25':10,'10:30':11,
            '10:35':12,'10:40':13,'10:45':14,'10:50':15,'10:55':16,'11:00':17,'11:05':18,'11:10':19,'11:15':20,'11:20':21,'11:25':22,
            '11:30':23,'13:05':24,'13:10':25,'13:15':26,'13:20':27,'13:25':28,'13:30':29,'13:35':30,'13:40':31,'13:45':32,'13:50':33,
            '13:55':34,'14:00':35,'14:05':36,'14:10':37,'14:15':38,'14:20':39,'14:25':40,'14:30':41,'14:35':42,'14:40':43,'14:45':44,
            '14:50':45,'14:55':46,'15:00':47,}

m15k_dict = {'09:45':0,'10:00':1,'10:15':2,'10:30':3,'10:45':4,'11:00':5,'11:15':6,'11:30':7,'13:15':8,'13:30':9,'13:45':10,'14:00':11,
             '14:15':12,'14:30':13,'14:45':14,'15:00':15,}

m30k_dict = {'10:00':0,'10:30':1,'11:00':2,'11:30':3,'13:30':4,'14:00':5,'14:30':6,'15:00':7}

m60k_dict = {'10:30':0,'11:30':1,'14:00':2,'15:00':3}


def get_update_counts(last_time,data_type):
    if data_type == 'm5k':
        last_day = datetime.datetime.strptime(last_time, '%Y-%m-%d %H:%M')
        days = last_day.strftime('%Y-%m-%d')
        minutes =  last_day.strftime('%H:%M')
        minutes_counts = 47-m5k_dict[minutes]
        date_list_update_counts = getBetweenDay(days)
        day_counts = count_trading_days(date_list_update_counts)
        total_update_counts = day_counts*48+minutes_counts
        if total_update_counts>1440:
            print('Attention!!! 缺失数据超过历史数据更新上限')
        else:
            pass
        return total_update_counts

    elif data_type == 'm15k':
        last_day = datetime.datetime.strptime(last_time, '%Y-%m-%d %H:%M')
        days = last_day.strftime('%Y-%m-%d')
        minutes =  last_day.strftime('%H:%M')
        minutes_counts = 15-m15k_dict[minutes]
        date_list_update_counts = getBetweenDay(days)
        day_counts = count_trading_days(date_list_update_counts)
        total_update_counts = day_counts*16+minutes_counts
        if total_update_counts>480:
            print('Attention!!! 缺失数据超过历史数据更新上限')
        else:
            pass
        return total_update_counts

    elif data_type == 'm30k':
        last_day = datetime.datetime.strptime(last_time, '%Y-%m-%d %H:%M')
        days = last_day.strftime('%Y-%m-%d')
        minutes =  last_day.strftime('%H:%M')
        minutes_counts = 7-m30k_dict[minutes]
        date_list_update_counts = getBetweenDay(days)
        day_counts = count_trading_days(date_list_update_counts)
        total_update_counts = day_counts*8+minutes_counts
        if total_update_counts>240:
            print('Attention!!! 缺失数据超过历史数据更新上限')
        else:
            pass
        return total_update_counts

    elif data_type == 'k':
        date_list_update_counts = getBetweenDay(last_time)
        day_counts = count_trading_days(date_list_update_counts)
        return day_counts

    else:
        last_day = datetime.datetime.strptime(last_time, '%Y-%m-%d %H:%M')
        days = last_day.strftime('%Y-%m-%d')
        minutes =  last_day.strftime('%H:%M')
        minutes_counts = 3-m60k_dict[minutes]
        date_list_update_counts = getBetweenDay(days)
        day_counts = count_trading_days(date_list_update_counts)
        total_update_counts = day_counts*4+minutes_counts
        if total_update_counts>120:
            print('Attention!!! 缺失数据超过历史数据更新上限')
        else:
            pass
        return total_update_counts
