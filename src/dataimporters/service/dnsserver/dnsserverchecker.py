#!/usr/bin/python3
#-*-coding:utf-8-*-

# pip3 install --user dnspython

import sys
sys.path.append('../common')
from service import *

import time
import datetime

import dns.resolver

class DNSServerChecker(QueryService):
    REQUEST_TIMEOUT = 10

    dns_ip = ""
    idc_name = ""
    def __init__(self):
        super(QueryService,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("DNSServerChecker")

    def prepare(self):
        DataImporter.prepare(self)
        return True

    def execute(self):
        resolver = dns.resolver.Resolver(configure=False)
        resolver.nameservers = [self.dns_ip]

        tick_start = time.time()

        resolve_ok = True
        try:
            answer = resolver.query('baidu.com', "NS", lifetime=self.REQUEST_TIMEOUT)
        except Exception as err:
            self.log(self.LOGE, "Resolve error of dns: %s %s" % (self.dns_ip, self.idc_name))
            self.log(self.LOGE, err)
            resolve_ok = False

        if (resolve_ok == False):
            return False

        if (len(answer) == 0):
            self.log(self.LOGE, "Resolve result empty of dns: %s %s" % (self.dns_ip, self.idc_name))
            return False

        for a in answer:
            print (a)

        total_seconds = time.time() - tick_start
        total_seconds = "%.2f" % total_seconds

        tick_now = datetime.datetime.now()
        tick_now = tick_now.replace(microsecond=0,second=0)
        str_minute_now = tick_now.strftime('%Y-%m-%d %H:%M')
        # 导入到数据库
        self.write_cursor.execute("INSERT INTO service_dns (log_time, dns_name, resolve_seconds) VALUES (%s, %s, %s)", (tick_now, self.idc_name + " " + self.dns_ip, total_seconds))

        self.write_conn.commit()

if __name__ == "__main__":
    print ("DNS Server Checker V1.0")
    print ("(C) 2019 Hunan MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")

    obj = DNSServerChecker()
    if (len(sys.argv) != 3):
        print("Usage: python3 ./dnsserverchecker.py [dns ip] [idc name]")
        sys.exit(0)

    obj.dns_ip = sys.argv[1]
    obj.idc_name = sys.argv[2]

    try:
        obj.run()
    except Exception as err:
        print (err)
    
