#!/usr/bin/python3
#-*-coding:utf-8-*-

import sys
sys.path.append('../common')
from service import *

import time
import datetime
import os

class DNSServer(QueryService):
    def __init__(self):
        super(QueryService,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("DNSServer")

    def prepare(self):
        DataImporter.prepare(self)
        return True

    def execute(self):
        f = open("./dnsservers.txt", "r")
        lines = f.readlines()
        f.close()

        for line in lines:
            args = line.replace("\n", "").split(",")
            #print (args)
            os.system("python3 ./dnsserverchecker.py %s %s &" % (args[1], args[0]))

if __name__ == "__main__":
    print ("DNS Server V1.0")
    print ("(C) 2019 Hunan MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = DNSServer()
    obj.run_forever(60)
    
