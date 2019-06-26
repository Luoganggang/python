#!/usr/bin/python3
#-*-coding:utf-8-*-

import sys
sys.path.append('../common')
from operation import *

import datetime
import redis

class CustomerQuery(DataImporter):
    def __init__(self):
        super(DataImporter,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("CustomerQuery")

    def execute(self):
        current_tick = datetime.datetime.now()

        input_channel = u'小能'
        # 从Redis获取数据
        r = redis.Redis(host='10.0.16.29', port=6379, decode_responses=True)
        session_total = r.get("csqueue_xn_totalchar:")
        r.connection_pool.disconnect()
        print(session_total)

        r = redis.Redis(host='10.0.16.30', port=6379, decode_responses=True)
        session_limit = r.get("csqueue_xn_totalchar_limit:")
        r.connection_pool.disconnect()
        print(session_limit)

        r = redis.Redis(host='10.0.16.29', port=6379, decode_responses=True)
        pass_total = r.get("csqueue_pass_count:")
        r.connection_pool.disconnect()
        print(pass_total)

        r = redis.Redis(host='10.0.16.17', port=6379, decode_responses=True)
        refuse_total = r.get("csqueue_refuse_count:")
        r.connection_pool.disconnect()
        print(refuse_total)

        # 导入到数据库
        self.write_cursor.execute('INSERT INTO service_customer_query (log_time, input_channel, session_total, session_limit, pass_total, refuse_total) VALUES (%s, %s, %s, %s, %s, %s)', \
            (current_tick, input_channel, session_total, session_limit, pass_total, refuse_total))
        self.write_conn.commit()

if __name__ == "__main__":
    print ("Customer Query V1.0")
    print ("(C) 2019 Hunan MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = CustomerQuery()
    obj.run_forever(60, run_first = True)
    
