#!/usr/bin/python3
#-*-coding:utf-8-*-

import sys
sys.path.append('../common')
from service import *

import time
import datetime
import os

class ClientDelay(QueryService):
    def __init__(self):
        super(QueryService,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("ClientDelay")

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

        # 获取所有省份清单
        sql = "select prvc from to_rf_cli_ntwk_dlay "
        sql = sql +"where (srvr_tm >= to_timestamp('%s', 'yyyy-MM-dd HH24:MI:SS') " % str_start
        sql = sql + "and srvr_tm <= to_timestamp('%s', 'yyyy-MM-dd HH24:MI:SS')) "% str_end
        sql = sql + "and prvc != '0' "
        sql = sql + "and cntry = '中国' and avg_tm >= 1 and avg_tm <= 200 "
        sql = sql + "group by prvc "
        self.read_cursor_dict_error_code.execute(sql)
        rows = self.read_cursor_dict_error_code.fetchall()
        #print (rows)
        provinces = []
        for item in rows:
            provinces.append(item[0])

        provinces.sort()
        #print (provinces)

        carriers = ['移动', '联通', '电信', '平均']
        # 获取每个省份到各机房的平均延时
        print ("Total SQL: %d" % (len(idcs) * len(provinces) * len(carriers)))

        tick_start = time.time()


        output_filename = "cnd_" + str_start  + ".csv"
        output_filename = output_filename.replace(":", "-")
        output_filename = output_filename.replace(" ", "_")
        # do not save
        output_filename = "/dev/null"


        #os.system("rm -f %s" % (output_filename))
        os.system("echo '微算互联' >> %s" % (output_filename))
        os.system("echo '客户端到各IDC延时报表' >> %s" % (output_filename))
        os.system("echo "" >> %s" % (output_filename))

        # 各IDC
        for idc in idcs:
            os.system("echo %s >> %s" % (idc_names[idc], output_filename))
            os.system('echo -n "运营商/地区," >> %s' % (output_filename))
            for province in provinces:
                os.system('echo -n "%s," >> %s' % (province, output_filename))
            os.system('echo "" >> %s' % (output_filename))

            # 各省份，各运营商
            for carrier in carriers:
                if (carrier != ' '):
                    os.system('echo -n "%s," >> %s' % (carrier, output_filename))
                else:
                    os.system('echo -n "平均," >> %s' % (output_filename))

                for province in provinces:
                    print ("[%s] <--[%s]-- [%s]" % (idc_names[idc], carrier, province))

                    sql = "select avg(avg_tm) from to_rf_cli_ntwk_dlay "
                    sql = sql +"where (srvr_tm >= to_timestamp('%s', 'yyyy-MM-dd HH24:MI:SS') " % str_start
                    sql = sql + "and srvr_tm <= to_timestamp('%s', 'yyyy-MM-dd HH24:MI:SS')) "% str_end
                    if (carrier != '平均'):
                        sql = sql + "and carr_oper = '%s' " % carrier
                    sql = sql + "and prvc = '%s' and idc_cd = '%s' " % (province, idc)
                    sql = sql + "and cntry = '中国' and avg_tm >= 1 and avg_tm <= 200 ;"

                    self.read_cursor_dict_error_code.execute(sql)
                    rows = self.read_cursor_dict_error_code.fetchall()
                    print (rows[0][0])
                    if ((rows[0] == None) or (rows[0][0] == None)):
                        # 无数据
                        os.system('echo -n "," >> %s' % (output_filename))
                        continue
                    else:
                        os.system('echo -n "%d," >> %s' % (rows[0][0], output_filename))

                    # 保存到本地数据库
                    self.write_cursor.execute('INSERT INTO network_client_delay (log_time, source_province, dest_idc, carrier, avg_time) VALUES (%s, %s, %s, %s, %s)', (str_start, province, idc_names[idc], carrier, '%d' % rows[0][0]))
                    self.write_conn.commit()

                # 换行
                os.system('echo "" >> %s' % (output_filename))

            # 换行
            os.system('echo "" >> %s' % (output_filename))

        os.system('echo "自动运维机器人生成于：" >> %s' % (output_filename))
        os.system('echo "%s" >> %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), output_filename))

        total_seconds = time.time() - tick_start
        print ("Total: %d seconds" % (total_seconds))
        os.system('echo "运行时长：%d秒" >> %s' % (total_seconds, output_filename))

        return True

if __name__ == "__main__":
    print ("Error Code Total By IDC V1.0")
    print ("(C) 2019 Hunan MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = ClientDelay()
    obj.run_by_hour(run_first = True)
    
