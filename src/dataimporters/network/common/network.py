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

# 用户上报网络错误的数据库
ERROR_CODE_DB_IP = "10.0.16.154"
ERROR_CODE_DB_PORT = 5432
ERROR_CODE_DB_LOGIN_USERNAME = "app_mon"
ERROR_CODE_DB_LOGIN_PASSWORD = "3efwi5j"
ERROR_CODE_DB_NAME = "rfdw"

class QueryNetworkErrorCode(DataImporter):
    def __init__(self):
        super(DataImporter,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("QueryNetworkError")

    def prepare(self):
        DataImporter.prepare(self)

        try:
            self.read_conn = psycopg2.connect(database=ERROR_CODE_DB_NAME,
                user=ERROR_CODE_DB_LOGIN_USERNAME,
                password=ERROR_CODE_DB_LOGIN_PASSWORD,
                host=ERROR_CODE_DB_IP,
                port=ERROR_CODE_DB_PORT,
                connect_timeout=REQUEST_TIMEOUT) 
                
            self.read_cursor_dict = self.read_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self.read_cursor = self.read_conn.cursor()
        except Exception as err:
            self.log(self.LOGE, err)
            return False

    def execute(self):
        pass

    def complete(self):
        DataImporter.complete(self)

        try:
            if (self.read_conn != None):
                self.read_conn.close()
        except Exception as err:
            pass

        try:
            if (self.read_cursor_dict != None):
                self.read_cursor_dict.close()
        except Exception as err:
            pass

        try:
            if (self.read_cursor != None):
                self.read_cursor.close()
        except Exception as err:
            pass
