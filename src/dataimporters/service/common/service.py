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

# 用户上报错误码的数据库
ERROR_CODE_DB_IP = "10.0.16.155"
ERROR_CODE_DB_PORT = 5432
ERROR_CODE_DB_LOGIN_USERNAME = "viewer"
ERROR_CODE_DB_LOGIN_PASSWORD = "e4ycCeqc4D"
ERROR_CODE_DB_NAME = "rfdw"

# 信息数据库
INFO_DB_IP = "10.0.16.223"
INFO_DB_PORT = 5432
INFO_DB_LOGIN_USERNAME = "read_only"
INFO_DB_LOGIN_PASSWORD = "wefi&234"
INFO_DB_NAME = "redfinger"

class QueryService(DataImporter):
    def __init__(self):
        super(DataImporter,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("QueryService")

    def connect_error_code_db(self):
        try:
            self.read_conn_error_code = psycopg2.connect(database=ERROR_CODE_DB_NAME,
                user=ERROR_CODE_DB_LOGIN_USERNAME,
                password=ERROR_CODE_DB_LOGIN_PASSWORD,
                host=ERROR_CODE_DB_IP,
                port=ERROR_CODE_DB_PORT,
                connect_timeout=REQUEST_TIMEOUT) 
                
            self.read_cursor_dict_error_code = self.read_conn_error_code.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self.read_cursor_error_code = self.read_conn_error_code.cursor()
        except Exception as err:
            self.log(self.LOGE, err)
            return False

    def disconnect_error_code_db(self):
        try:
            if (self.read_conn_error_code != None):
                self.read_conn_error_code.close()
        except Exception as err:
            pass

        try:
            if (self.read_cursor_dict_error_code != None):
                self.read_cursor_dict_error_code.close()
        except Exception as err:
            pass

        try:
            if (self.read_cursor_error_code != None):
                self.read_cursor_error_code.close()
        except Exception as err:
            pass

    def connect_info_db(self):
        try:
            self.read_conn_info = psycopg2.connect(database=INFO_DB_NAME,
                user=INFO_DB_LOGIN_USERNAME,
                password=INFO_DB_LOGIN_PASSWORD,
                host=INFO_DB_IP,
                port=INFO_DB_PORT,
                connect_timeout=REQUEST_TIMEOUT) 
                
            self.read_cursor_dict_info = self.read_conn_info.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self.read_cursor_info = self.read_conn_info.cursor()
        except Exception as err:
            self.log(self.LOGE, err)
            return False

    def disconnect_info_db(self):
        try:
            if (self.read_conn_info != None):
                self.read_conn_info.close()
        except Exception as err:
            pass

        try:
            if (self.read_cursor_dict_info != None):
                self.read_cursor_dict_info.close()
        except Exception as err:
            pass

        try:
            if (self.read_cursor_info != None):
                self.read_cursor_info.close()
        except Exception as err:
            pass

    def prepare(self):
        DataImporter.prepare(self)
        return True

    def execute(self):
        pass

    def complete(self):
        DataImporter.complete(self)
        self.disconnect_error_code_db()
        self.disconnect_info_db()
