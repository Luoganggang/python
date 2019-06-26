#!/usr/bin/python3
#-*-coding:utf-8-*-

import sys
sys.path.append('../common')
from network import *

import time
import datetime

class ErrorCodeByProvince(QueryNetworkErrorCode):
    def __init__(self):
        super(QueryNetworkErrorCode,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("ErrorCodeByProvince")

    def execute(self):
        tick_now = datetime.datetime.now()
        tick_now = tick_now.replace(microsecond=0,second=0)
        str_minute_now = tick_now.strftime('%Y-%m-%d %H:%M')

        tick_minute_ago = tick_now - datetime.timedelta(minutes = 1)
        str_minute_ago = tick_minute_ago.strftime('%Y-%m-%d %H:%M')     
        print (str_minute_now)
        print (str_minute_ago)
        #return True

        # 从用户上传网络错误码数据库获取数量
        sql = "select count(*),province from rf_client_netinfo "
        sql = sql +"where (server_time >= to_timestamp('%s', 'yyyy-MM-dd HH24:MI') " % str_minute_ago
        sql = sql + "and server_time < to_timestamp('%s', 'yyyy-MM-dd HH24:MI') "% str_minute_now
        sql = sql +" and country='中国') "
        sql = sql +" group by province"


        self.read_cursor_dict.execute(sql)
        rows = self.read_cursor_dict.fetchall()
        print (rows)

        # 导入到数据库
        for row in rows:
            self.write_cursor.execute('INSERT INTO network_error_by_province (log_time, province, total) VALUES (%s, %s, %s)', (tick_now, row['province'], row['count']))

        self.write_conn.commit()

        #self.rows = self.cursor.execute(sql)
        #self.rows = self.cursor.fetchall()

if __name__ == "__main__":
    print ("Error Code Total By Province V1.0")
    print ("(C) 2019 Hunan MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = ErrorCodeByProvince()
    obj.run_forever(60, run_first = False)
    
