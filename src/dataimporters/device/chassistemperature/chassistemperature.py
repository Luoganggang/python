#!/usr/bin/python3
#-*-coding:utf-8-*-

import sys
sys.path.append('../common')
from device import *

import datetime

class ChassisTemperature(DataImporter):
    def __init__(self):
        super(DataImporter,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("ChassisTemperature")

    def prepare(self):
        DataImporter.prepare(self)

    def execute(self):
        tick_now = datetime.datetime.now()
        tick_10min_ago = tick_now - datetime.timedelta(minutes = 10)
        str_end = tick_now.strftime('%Y-%m-%d %H:%M:00')
        str_start = tick_10min_ago.strftime('%Y-%m-%d %H:%M:00')
        print (str_start)
        print (str_end)

        if (self.connect_db_device_status() == False):
            self.log(self.LOGE, "connect to device status db error")
            return False

        if (self.connect_db_device_info() == False):
            self.log(self.LOGE, "connect to device info db error")
            return False

        sql = "SELECT substring(device_ip, '\d+.\d+.\d+') as ip_segment, avg(temperature) "
        sql = sql + "FROM rf_device_monitor_info_current "
        sql = sql + "where update_time >= '%s' and update_time < '%s' " % (str_start, str_end)
        sql = sql + "GROUP BY ip_segment "

        self.read_cursor_dict_device_status.execute(sql)
        rows = self.read_cursor_dict_device_status.fetchall()
        #print (rows)
        map_ip_segments = {}
        dict_ip_segments = []
        for row in rows:
            if ((row[0] != None) and (row[1] != None)):
                map_ip_segments[row[0]] = int(row[1])
                dict_ip_segments.append(row[0])

        #print (map_ip_segments)
        for ip_segment in dict_ip_segments:
            # 获取每个机箱对应的IDC
            ip = ip_segment + ".1"
            sql = "select cabinet_info from rf_device where device_ip = '%s'" % ip
            self.read_cursor_dict_device_info.execute(sql)
            rows = self.read_cursor_dict_device_info.fetchall()

            # 导入到数据库
            print (ip, rows)
            if ((len(rows) == 0) or (rows[0] == None)):
                idc = "未知IDC"
            else:
                idc = rows[0][0]
            sql = "INSERT INTO device_temperature_chassis "
            sql = sql + "(log_time, ip_segment, idc, avg_temperature) VALUES "
            sql = sql + " (%s, %s, %s, %s)"
            self.write_cursor.execute(sql, (tick_now, ip_segment, idc, map_ip_segments[ip_segment]))

        self.write_conn.commit()
        return True

    def complete(self):
        DataImporter.complete(self)


if __name__ == "__main__":
    print ("Chassis Temperature V1.0")
    print ("(C) 2019 Guangzhou MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = ChassisTemperature()
    obj.run_by_10min(run_first = True)
    
