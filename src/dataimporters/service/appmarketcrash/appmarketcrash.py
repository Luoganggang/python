#!/usr/bin/python3
#-*-coding:utf-8-*-

# !!!注意：本机需要与10.0.16.69提前建立ssh免密码登录：
# 本机上运行：
# cd ~/.ssh
# ssh-keygen -t rsa
# ssh-copy-id -i ./id_rsa viewer@10.0.16.69
# 输入密码，然后 ssh viewer@10.0.16.69测试可以免密码登录

import sys
import os
sys.path.append('../common')
from service import *

import time
import datetime

class AppMarketCrash(QueryService):

    def __init__(self):
        super(QueryService,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("AppMarketCrash")

    def prepare(self):
        DataImporter.prepare(self)
        self.log(self.LOGD, "connecting to info db...")
        if (self.connect_info_db() == False):
            return False

        return True
       
    def execute(self):
        local_filename = "./lastcrash.txt"

        # 等待服务器一段时间生成日志文件
        self.log(self.LOGD, "sleep 1 minute to wait server generate log file...")
        time.sleep(60)

        tick_now = datetime.datetime.now()
        tick_now = tick_now.replace(microsecond=0,second=0)
        str_minute_now = tick_now.strftime('%Y-%m-%d %H:%M')

        tick_minute_ago = tick_now - datetime.timedelta(minutes = 1)
        str_minute_ago = tick_minute_ago.strftime('%Y-%m-%d %H:%M')     
        #print (str_minute_now)
        #print (str_minute_ago)

        # 复制服务器日志文件到本地
        now_minute = int(datetime.datetime.now().minute / 10) * 10
        server_filename = "market_crash_%s" % (datetime.datetime.now().replace(minute=now_minute).strftime("%Y%m%d%H%M"))
        self.log(self.LOGD, "copy file from server..." + server_filename)
        #print (server_filename)

        os.system("rm -f " + local_filename)
        os.system("scp viewer@10.0.16.69:/data/yw_opert/app_fail/log/%s %s" % (server_filename, local_filename))

        if (os.path.exists(local_filename) == False):
            self.log(self.LOGI, "local crash file not exits")
            return True

        f = open(local_filename, "r")
        logs = f.readlines()
        f.close()

        self.log(self.LOGD, "processing crash logs, total %d " % len(logs))
        for log in logs:
            if (self.process_log(log) == False):
                return False

        return True

    def get_idc_name(self, vm_ip):
        sql = "select idc_name from rf_pad inner join rf_idc on rf_idc.idc_id=rf_pad.idc_id where pad_ip='%s'" % vm_ip
        self.read_cursor_dict_info.execute(sql)
        rows2 = self.read_cursor_dict_info.fetchall()
        if (len(rows2) == 0):
            return "未知机房"
        if (len(rows2[0]) == 0):
            return "未知机房"

        return rows2[0][0]
               
    def process_log(self, log):
        start_pos = log.find("market_crash")
        if (start_pos == -1):
            self.log(self.LOGI, "cannot find 'market_crash', skipped log")
            return True

        # 去掉前缀
        real_log = log[start_pos + len('market_crash') + 1:]
        #print (real_log)

        # 提取stackInfo
        stack_info_pos = real_log.find("stackInfo=")
        if (stack_info_pos == -1):
            self.log(self.LOGI, "cannot find stackInfo skipped log")
            return True

        stack_info = real_log[stack_info_pos + len("stackInfo="):]
        #print (stack_info)

        real_log = real_log[:stack_info_pos]
        #print (real_log)

        log_items = real_log.split(",")
        #print (log_items)

        # key:value
        map_logs = {}
        for log_item in log_items:
            if (log_item == ""):
                continue

            line = log_item.split("=")
            if (len(line) < 2):
                line_value = "null"
            else:
                line_value = line[1]

            map_logs[line[0]] = line_value

        map_logs['errorType'] = 'crash'
        map_logs['idc'] = self.get_idc_name(map_logs['ip'])
        map_logs['stackInfo'] = stack_info
        #print (map_logs)

        # 将此条日志插入到数据库
        return self.insert_log(map_logs)

    def insert_log(self, map_logs):
        # 导入到数据库
        sql = "INSERT INTO service_app_market_error "
        sql = sql + "(log_time, error_type, operate_type, vm_ip, idc, "
        sql = sql + " os_type, market_version, device_version, game_id, "
        sql = sql + " package_name, app_name, error_code, api_code, stack_info) "
        sql = sql + " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "

        parameters = []
        parameters.append(map_logs['date'])                   
        parameters.append(map_logs['errorType'])                   
        #parameters.append(map_logs['operateType'])
        parameters.append('')                   
        parameters.append(map_logs['ip'])
        parameters.append(map_logs['idc'])
        parameters.append(map_logs['osType'])
        parameters.append(map_logs['marketVersion'])
        parameters.append(map_logs['deviceVersion'])
        #parameters.append(map_logs['gameId'])
        parameters.append('')                   
        #parameters.append(map_logs['packageName'])
        parameters.append('')                   
        #parameters.append(map_logs['appName'])
        parameters.append('')                   
        #parameters.append(map_logs['errorCode'])
        parameters.append('')                   
        #parameters.append(map_logs['apiCode'])
        parameters.append('')                   
        parameters.append(map_logs['stackInfo'])

        self.write_cursor.execute(sql, parameters)
        self.write_conn.commit()
        return True

if __name__ == "__main__":
    print ("App Market Crash V1.0")
    print ("(C) 2019 Guangzhou MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = AppMarketCrash()
    obj.run_by_10min()
    
