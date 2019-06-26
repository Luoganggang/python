#!/usr/bin/python3
#-*-coding:utf-8-*-

import sys
sys.path.append('../common')
from device import *

import datetime

class DeviceOfflineVM(QueryDeviceBalance):
    def __init__(self):
        super(DeviceOfflineVM,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("DeviceOfflineVM")

    def execute(self):
        current_tick = datetime.datetime.now()

        # 使用机器人API从后台获取可用设备总数
        client=requests.session()
        headers = {'Connection': 'keep-alive'}

        params = {"enableStatus":"1","faultStatus":"0","grantOpenStatus":"on","maintStatus":"0", \
            "vmStatus":"0","padStatus":"1","rows":"1","padClassify":"1","padPool":"1","storageType":"0"}

        page = 1
        while (True):
            url = "/hsz/robot/post/findPadList?"
            params.update(FIXED_REQUEST)
            params.update({"pageNo":("%d" % page), "pageSize":"999"})
            url = get_update_url(QUERY_BALANCE_SERVER_URL + url, params, QUERY_BALANCE_APP_KEY)

            rep=client.post(url, timeout = REQUEST_TIMEOUT * 2)
            data = rep.text
            #print data
            lists = json.loads(data)        

            if (page == 1):
                all_total = lists['response']['totalNumber']
                print ("Received total %d" % all_total)

            padlist = lists['response']['padList']
            for pad in padlist:
                if (pad['deviceIp'].find("10.70.") == 0):
                    all_total = all_total - 1

            page = page +1

            if (lists['response']['hasNextPage'] == False):
                break;

        print ("result: %d" % all_total)

        # 导入到数据库
        self.write_cursor.execute('INSERT INTO device_offline_vm (log_time, total) VALUES (%s, %s)', \
            (current_tick, all_total))
        self.write_conn.commit()

if __name__ == "__main__":
    print ("Device Offline VM V1.0")
    print ("(C) 2019 Hunan MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = DeviceOfflineVM()
    obj.run_forever(DEVICE_REFRESH_INTERVAL)
    
