#!/usr/bin/python3
#-*-coding:utf-8-*-

import sys
sys.path.append('../common')
from service import *

import time
import datetime

class ErrorCodeByIDC(QueryService):
    def __init__(self):
        super(QueryService,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("ErrorCodeByIDC")

    def prepare(self):
        DataImporter.prepare(self)
        self.log(self.LOGD, "connecting to info db...")
        if (self.connect_info_db() == False):
            return False

        self.log(self.LOGD, "connecting to error code db...")
        if (self.connect_error_code_db() == False):
            return False

        return True

    def execute(self):
        tick_now = datetime.datetime.now()
        tick_now = tick_now.replace(microsecond=0,second=0)
        str_minute_now = tick_now.strftime('%Y-%m-%d %H:%M')

        tick_minute_ago = tick_now - datetime.timedelta(minutes = 1)
        str_minute_ago = tick_minute_ago.strftime('%Y-%m-%d %H:%M')     
        #print (str_minute_now)
        #print (str_minute_ago)

        # 从用户上传错误码数据库获取VM ID
        sql = "select distinct pad_cd from to_rf_pad_play_log "
        sql = sql +"where (log_time >= to_timestamp('%s', 'yyyy-MM-dd HH24:MI') " % str_minute_ago
        sql = sql + "and log_time < to_timestamp('%s', 'yyyy-MM-dd HH24:MI')) "% str_minute_now

        self.read_cursor_dict_error_code.execute(sql)
        rows = self.read_cursor_dict_error_code.fetchall()
        #print (rows)

        map_idcs = {}

        # 获取每个VM ID对应的IDC名
        for row in rows:
            #print (row)
            if (len(row) == 0):
                continue

            if (len(row[0]) == 0):
                continue

            sql = "select pad_code,idc_name from rf_pad inner join rf_idc on rf_idc.idc_id=rf_pad.idc_id where pad_code='%s'" % row[0];
            self.read_cursor_dict_info.execute(sql)
            rows2 = self.read_cursor_dict_info.fetchall()
            #print (rows2)

            for row2 in rows2:
                if (len(row2) != 2):
                    continue

                idc_name = row2[1]
                if (idc_name in map_idcs.keys()):
                    map_idcs[idc_name] = map_idcs[idc_name] + 1
                else:
                    map_idcs[idc_name] = 1

        #print (map_idcs)
        for idc in map_idcs.keys():
            #print (idc)
            # 导入到数据库
            self.write_cursor.execute('INSERT INTO service_user_error_by_idc (log_time, idc_name, total) VALUES (%s, %s, %s)', (tick_now, idc, map_idcs[idc]))

        self.write_conn.commit()
        return True

if __name__ == "__main__":
    print ("Error Code Total By IDC V1.0")
    print ("(C) 2019 Hunan MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = ErrorCodeByIDC()
    obj.run_forever(60, run_first = False)
    
