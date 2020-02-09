#首次运行创建dfcf数据库
import pymysql
from config import *

#创建本地数据库连接，并建立新的数据库
#建立本地数据库连接(需要先开启数据库服务)
db = pymysql.connect(db_host, usr_name, db_password, charset='utf8mb4')
cursor = db.cursor()

#创建数据库dfcf，如果存在则跳过
sqlSentence1 = "create database if not exists dfcf"
cursor.execute(sqlSentence1)#选择使用当前数据库
sqlSentence2 = "use dfcf;"
cursor.execute(sqlSentence2)