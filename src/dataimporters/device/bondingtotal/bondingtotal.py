#!/usr/bin/python3
#-*-coding:utf-8-*-

import sys
sys.path.append('../common')
from device import *

import datetime

class BondingTotal(QueryDeviceBalance):
    def __init__(self):
        super(QueryDeviceBalance,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("BondingTotal")

    def execute(self):
        current_tick = datetime.datetime.now()

        # 使用机器人API从后台获取可用设备总数
        client=requests.session()
        headers = {'Connection': 'keep-alive'}

        params = {"bindStatus":"1", "rows":"1"}
        params.update(FIXED_REQUEST)
        url = "/hsz/robot/post/findPadList?"
        url = get_update_url(QUERY_BALANCE_SERVER_URL + url, params, QUERY_BALANCE_APP_KEY)
        rep=client.post(url,  timeout=REQUEST_TIMEOUT)
        data = rep.text
        lists = json.loads(data)        
        bonding_total = lists['response']['totalNumber']
        print ("new api bonding total: %d" % bonding_total)

        # 导入到数据库
        self.write_cursor.execute('INSERT INTO operation_bonding_total (log_time, bonding_total) VALUES (%s, %s)', \
            (current_tick, bonding_total))
        self.write_conn.commit()

if __name__ == "__main__":
    print ("Bonding Total V1.0")
    print ("(C) 2019 Hunan MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = BondingTotal()
    obj.run_forever(600)
    
