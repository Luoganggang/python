#!/usr/bin/python3
#-*-coding:utf-8-*-

import sys
sys.path.append('../common')
from service import *

import time
import datetime

class ExpiredDeviceTotal(QueryService):
    def __init__(self):
        super(QueryService,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("ExpiredDeviceTotal")

    def prepare(self):
        DataImporter.prepare(self)
        self.log(self.LOGD, "connecting to info db...")
        if (self.connect_info_db() == False):
            return False

        return True

    def execute(self):
        tick_now = datetime.datetime.now()
        tick_now = tick_now.replace(microsecond=0,second=0)
        str_start_time = tick_now.strftime('%Y-%m-%d %H:%M:00')

        tick_10minute_later = tick_now + datetime.timedelta(minutes = 10)
        str_end_time = tick_10minute_later.strftime('%Y-%m-%d %H:%M:00')
        print (str_start_time)
        print (str_end_time)

        sql = "SELECT idc_name,pad_grade,count(*) FROM rf_user_pad a "
        sql = sql + "JOIN rf_pad b ON a.pad_id = b.pad_id "
        sql = sql + "JOIN rf_idc c ON b.idc_id = c.idc_id "
        sql = sql + "where expire_time >='%s' and expire_time<'%s' " % (str_start_time, str_end_time)
        sql = sql + "group by idc_name,pad_grade"
        self.read_cursor_dict_info.execute(sql)
        rows2 = self.read_cursor_dict_info.fetchall()
        print (rows2)

        # 导入到数据库
        for row in rows2:
            idc_name = row[0]

            if (row[1] == '0'):
                device_type = '普通设备'
            elif (row[1] == '1'):
                device_type = 'VIP'
            elif (row[1] == '2'):
                device_type = '体验设备'
            elif (row[1] == '3'):
                device_type = 'SVIP'
            elif (row[1] == '5'):
                device_type = 'GVIP'
            else:
                device_type = '未知'

            total = row[2]

            self.write_cursor.execute('INSERT INTO operation_expired_device_total (log_time, idc_name, device_type, total) VALUES (%s, %s, %s, %s)', (str_start_time, idc_name, device_type, total))

        self.write_conn.commit()
        return True

if __name__ == "__main__":
    print ("Expired Device Total V1.0")
    print ("(C) 2019 Guangzhou MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = ExpiredDeviceTotal()
    obj.run_by_10min()
    
