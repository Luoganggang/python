#!/usr/bin/python3
#-*-coding:utf-8-*-

import requests
import psycopg2
import psycopg2.extras
import sys
import hashlib
import json


sys.path.append('../../common')
from dataimporters import DataImporter

REQUEST_TIMEOUT = 10
DEVICE_REFRESH_INTERVAL = 60

# 用户上报错误码的数据库
DB_IP = "10.0.16.155"
DB_PORT = 5432
#DB_LOGIN_USERNAME = "rfdw"
#DB_LOGIN_PASSWORD = "YR34T0or*Hm"
DB_LOGIN_USERNAME = "viewer"
DB_LOGIN_PASSWORD = "e4ycCeqc4D"
DB_NAME = "rfdw"

# 机器人API服务器地址和APP KEY
QUERY_BALANCE_SERVER_URL = "http://10.0.16.91"
QUERY_BALANCE_APP_KEY = "HSZ@s0zRId!fiN3er8"

FIXED_REQUEST = {"clientId":"100000001", \
"v":"2.1.11", \
"platform":"1", \
"sysVersion":"6.0.1", \
"channelCode":"com.redfinger.app", \
"pageSize":"200"}

def get_update_url(input_host, input_data_map, app_key):
    keys = sorted(input_data_map.keys())
    url = ""
    first_node = True
    for data in keys:
        #print data
        if (first_node):
            url = data + "=" + input_data_map[data]
            first_node = False
        else:
            url = url + "&" + data + "=" + input_data_map[data]
            
    url_to_calculate = url + app_key
    #print url
    
    m1 = hashlib.md5()
    m1.update(url_to_calculate.encode("utf-8"))
    digested = m1.hexdigest()
    digested = digested[8:24]
    
    url = input_host + url + "&cliSign=" + digested
    #print "updated url"
    #print url
    return url


class QueryDeviceBalance(DataImporter):
    def __init__(self):
        super(DataImporter,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("QueryDeviceBalance")

    def prepare(self):
        DataImporter.prepare(self)

    def execute(self):
        pass

    def complete(self):
        DataImporter.complete(self)
