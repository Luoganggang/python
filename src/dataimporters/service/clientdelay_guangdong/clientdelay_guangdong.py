#!/usr/bin/python3
#-*-coding:utf-8-*-

import sys
sys.path.append('../common')
from service import *

import time
import datetime
import os

class ClientDelayGuangdong(QueryService):
    def __init__(self):
        super(QueryService,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("ClientDelayGuangdong")

    def prepare(self):
        DataImporter.prepare(self)

        self.log(self.LOGD, "connecting to error code db...")
        if (self.connect_error_code_db() == False):
            return False

        return True

    def execute(self):
        tick_now = datetime.datetime.now()
        tick_now = tick_now - datetime.timedelta(hours = 1)
        tick_now = tick_now.replace(microsecond=0,second=0)
        str_start = tick_now.strftime('%Y-%m-%d %H:00:00')
        str_end = tick_now.strftime('%Y-%m-%d %H:59:59')
        print (str_start)
        print (str_end)

        """
        # 获取所有IDC清单
        sql = "select idc_cd from to_rf_cli_ntwk_dlay "
        sql = sql +"where (srvr_tm >= to_timestamp('%s', 'yyyy-MM-dd HH24:MI:SS') " % str_start
        sql = sql + "and srvr_tm <= to_timestamp('%s', 'yyyy-MM-dd HH24:MI:SS')) "% str_end
        sql = sql + "and cntry = '中国' and avg_tm >= 1 and avg_tm <= 200"
        sql = sql + "group by idc_cd "

        self.read_cursor_dict_error_code.execute(sql)
        rows = self.read_cursor_dict_error_code.fetchall()
        print (rows)
        idcs = rows
        idcs.sort()
        """
        idcs = []
        idc_names = {}
        f = open("./idcs.txt", "r")
        lines = f.readlines()
        f.close()
        for line in lines:
            items = line.replace("\n", "").split(",")
            idcs.append(items[0])
            idc_names[items[0]] = items[1]

        # 获取所有城市清单
        sql = "select cty from to_rf_cli_ntwk_dlay "
        sql = sql +"where (srvr_tm >= to_timestamp('%s', 'yyyy-MM-dd HH24:MI:SS') " % str_start
        sql = sql + "and srvr_tm <= to_timestamp('%s', 'yyyy-MM-dd HH24:MI:SS')) "% str_end
        sql = sql + "and cty != '0' "
        sql = sql + "and cntry = '中国' and prvc = '广东省' and avg_tm >= 1 and avg_tm <= 200 "
        sql = sql + "group by cty "
        self.read_cursor_dict_error_code.execute(sql)
        rows = self.read_cursor_dict_error_code.fetchall()
        #print (rows)
        cities = []
        for item in rows:
            cities.append(item[0])

        cities.sort()
        print (cities)

        carriers = ['移动', '联通', '电信', '平均']
        # 获取每个城市到各机房的平均延时
        print ("Total SQL: %d" % (len(idcs) * len(cities) * len(carriers)))

        tick_start = time.time()

        # 各IDC
        for idc in idcs:
            # 各省份，各运营商
            for carrier in carriers:
                for city in cities:
                    print ("[%s] <--[%s]-- [%s]" % (idc_names[idc], carrier, city))

                    sql = "select avg(avg_tm) from to_rf_cli_ntwk_dlay "
                    sql = sql +"where (srvr_tm >= to_timestamp('%s', 'yyyy-MM-dd HH24:MI:SS') " % str_start
                    sql = sql + "and srvr_tm <= to_timestamp('%s', 'yyyy-MM-dd HH24:MI:SS')) "% str_end
                    if (carrier != '平均'):
                        sql = sql + "and carr_oper = '%s' " % carrier
                    sql = sql + "and cty = '%s' and idc_cd = '%s' " % (city, idc)
                    sql = sql + "and cntry = '中国' and prvc = '广东省' and avg_tm >= 1 and avg_tm <= 200 ;"

                    self.read_cursor_dict_error_code.execute(sql)
                    rows = self.read_cursor_dict_error_code.fetchall()
                    print (rows[0][0])
                    if ((rows[0] == None) or (rows[0][0] == None)):
                        # 无数据
                        continue

                    # 保存到本地数据库
                    self.write_cursor.execute('INSERT INTO network_client_delay_guangdong (log_time, source_city, dest_idc, carrier, avg_time) VALUES (%s, %s, %s, %s, %s)', (str_start, city, idc_names[idc], carrier, '%d' % rows[0][0]))
                    self.write_conn.commit()

        total_seconds = time.time() - tick_start
        print ("Total: %d seconds" % (total_seconds))

        return True

if __name__ == "__main__":
    print ("Client Network Delay Guangdong V1.0")
    print ("(C) 2019 Guangzhou MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = ClientDelayGuangdong()
    obj.run_by_hour(run_first = False)
    
