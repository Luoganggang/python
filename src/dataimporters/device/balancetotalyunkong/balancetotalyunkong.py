#!/usr/bin/python3
#-*-coding:utf-8-*-

import sys
sys.path.append('../common')
from device import *

import datetime

class DeviceBalanceTotalYunKong(QueryDeviceBalance):
    def __init__(self):
        super(QueryDeviceBalance,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("DeviceBalanceTotalYunKong")

    def execute(self):
        current_tick = datetime.datetime.now()

        # 使用机器人API从后台获取可用设备总数
        client=requests.session()
        headers = {'Connection': 'keep-alive'}

        url = "/hsz/robot/post/findPadList?"
        params = {"enableStatus":"1","bindStatus":"0","faultStatus":"0","grantOpenStatus":"on","maintStatus":"0", \
            "vmStatus":"1","padStatus":"1","rows":"1","padClassify":"1","padPool":"2"}

        params.update(FIXED_REQUEST)
        url = get_update_url(QUERY_BALANCE_SERVER_URL + url, params, QUERY_BALANCE_APP_KEY)
        rep=client.post(url, timeout = REQUEST_TIMEOUT)
        data = rep.text
        #print data
        lists = json.loads(data)        
        all_total = lists['response']['totalNumber']
        #print ("new api total data: %d" % lists['response']['totalNumber'])

        # 导入到数据库
        self.write_cursor.execute('INSERT INTO device_balance_total_yunkong (log_time, balance) VALUES (%s, %s)', \
            (current_tick, all_total))
        self.write_conn.commit()

        #self.rows = self.cursor.execute(sql)
        #self.rows = self.cursor.fetchall()

if __name__ == "__main__":
    print ("Device Balance Total YunKong V1.0")
    print ("(C) 2019 Hunan MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = DeviceBalanceTotalYunKong()
    obj.run_forever(DEVICE_REFRESH_INTERVAL)
    
