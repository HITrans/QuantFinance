#此文件为sql相关函数

import pandas as pd
import pymysql
from sqlalchemy import create_engine

def read_table(cur, sql_order): # sql_order is a string
    try:
        cur.execute(sql_order) # 多少条记录
        data  = cur.fetchall(  )
        frame = pd.DataFrame(list(data))
    except:
        frame = pd.DataFrame()

    return frame

