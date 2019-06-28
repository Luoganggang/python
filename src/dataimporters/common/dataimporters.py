#!/usr/bin/python3
#-*-coding:utf-8-*-

import time
import psycopg2
import psycopg2.extras
import datetime

import sys
sys.path.append('../../../common')
import autooms

from abc import ABCMeta, abstractmethod

REQUEST_TIMEOUT = 10

# grafana读取的数据库，即导入的数据将会保存到这里
WRITE_DB_IP = "10.0.16.18"
WRITE_DB_PORT = 5432
WRITE_DB_LOGIN_USERNAME = "autooms"
WRITE_DB_LOGIN_PASSWORD = "88W23283@3eds"
WRITE_DB_NAME = "autooms"

# 设备信息数据库
READ_DB_DEVICE_INFO_IP = "10.0.16.223"
READ_DB_DEVICE_INFO_PORT = 5432
READ_DB_DEVICE_INFO_LOGIN_USERNAME = "read_only"
READ_DB_DEVICE_INFO_LOGIN_PASSWORD = "wefi&234"
READ_DB_DEVICE_INFO_NAME = "redfinger"

# 设备上报状态数据库
READ_DB_DEVICE_STATUS_IP = "10.0.16.131"
READ_DB_DEVICE_STATUS_PORT = 5432
READ_DB_DEVICE_STATUS_LOGIN_USERNAME = "cs_read"
READ_DB_DEVICE_STATUS_LOGIN_PASSWORD = "qwVH0gkyz8"
READ_DB_DEVICE_STATUS_NAME = "device_db"

class DataImporter(autooms.Base):

    def __init__(self):
        super(autooms.Base,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("DataImporter")

    def connect_db_device_status(self):
        try:
            self.read_conn_device_status = psycopg2.connect(database=READ_DB_DEVICE_STATUS_NAME,
                user=READ_DB_DEVICE_STATUS_LOGIN_USERNAME,
                password=READ_DB_DEVICE_STATUS_LOGIN_PASSWORD,
                host=READ_DB_DEVICE_STATUS_IP,
                port=READ_DB_DEVICE_STATUS_PORT,
                connect_timeout=REQUEST_TIMEOUT) 
                
            self.read_cursor_dict_device_status = self.read_conn_device_status.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self.read_cursor_device_status = self.read_conn_device_status.cursor()
        except Exception as err:
            self.log(self.LOGE, err)
            return False

    def disconnect_db_device_status(self):
        try:
            if (self.read_conn_device_status != None):
                self.read_conn_device_status.close()
                self.read_conn_device_status = None
        except Exception as err:
            pass

        try:
            if (self.read_cursor_dict_device_status != None):
                self.read_cursor_dict_device_status.close()
                self.read_cursor_dict_device_status = None
        except Exception as err:
            pass

        try:
            if (self.read_cursor_error_code != None):
                self.read_cursor_device_status.close()
                self.read_cursor_device_status = None
        except Exception as err:
            pass


    def connect_db_device_info(self):
        try:
            self.read_conn_device_info = psycopg2.connect(database=READ_DB_DEVICE_INFO_NAME,
                user=READ_DB_DEVICE_INFO_LOGIN_USERNAME,
                password=READ_DB_DEVICE_INFO_LOGIN_PASSWORD,
                host=READ_DB_DEVICE_INFO_IP,
                port=READ_DB_DEVICE_INFO_PORT,
                connect_timeout=REQUEST_TIMEOUT) 
                
            self.read_cursor_dict_device_info = self.read_conn_device_info.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self.read_cursor_device_info = self.read_conn_device_info.cursor()
        except Exception as err:
            self.log(self.LOGE, err)
            return False

    def disconnect_db_device_info(self):
        try:
            if (self.read_conn_device_info != None):
                self.read_conn_device_info.close()
                self.read_conn_device_info = None
        except Exception as err:
            pass

        try:
            if (self.read_cursor_dict_device_info != None):
                self.read_cursor_dict_device_info.close()
                self.read_cursor_dict_device_info = None
        except Exception as err:
            pass

        try:
            if (self.read_cursor_device_info != None):
                self.read_cursor_device_info.close()
                self.read_cursor_device_info = None
        except Exception as err:
            pass

    def prepare(self):
        try:
            self.write_conn = psycopg2.connect(database=WRITE_DB_NAME,
                user=WRITE_DB_LOGIN_USERNAME,
                password=WRITE_DB_LOGIN_PASSWORD,
                host=WRITE_DB_IP,
                port=WRITE_DB_PORT,
                connect_timeout=REQUEST_TIMEOUT) 
                
            self.write_cursor_dict = self.write_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self.write_cursor = self.write_conn.cursor()
        except Exception as err:
            self.log(self.LOGE, err)
            return False

    @abstractmethod
    def execute(self):
        pass

    def complete(self):
        try:
            if (self.write_cursor_dict != None):
                self.write_cursor_dict.close()
        except Exception as err:
            pass

        try:
            if (self.write_cursor != None):
                self.write_cursor.close()
        except Exception as err:
            pass

        try:
            if (self.write_conn != None):
                self.write_conn.close()
        except Exception as err:
            pass

        self.disconnect_db_device_status()
        self.disconnect_db_device_info()

    def run(self):
        # 导入数据准备
        self.log(self.LOGD, ">> prepare()")
        if (self.prepare() == False):
            self.log(self.LOGD, "<< prepare() returned False")
            return False
        self.log(self.LOGD, "<< prepare() returned True")

        # 导入数据
        self.log(self.LOGD, ">> execute()")
        if (self.execute() == False):
            self.log(self.LOGD, "<< execute() returned False")
            self.complete()
            return False
        self.log(self.LOGD, "<< execute() returned True")

        # 导入数据完成
        self.log(self.LOGD, ">> complete()")
        if (self.complete() == False):
            self.log(self.LOGD, "<< complete() returned False")
            return False
        self.log(self.LOGD, "<< complete() returned True")

        return True

    def run_forever(self, refresh_interval, run_first = True, max_keep_hours = 24 * 14):
        while (True):
            if (run_first):
                try:
                    self.run()
                except Exception as err:
                    self.log(self.LOGE, err)           

            run_first = True

            self.log(self.LOGI, "sleep %d seconds..." % refresh_interval)
            time.sleep(refresh_interval)

    def run_by_10min(self, run_first = False, max_keep_hours = 24 * 14):
        if (run_first):
            last_min = -1
        else:
            last_min = int(datetime.datetime.now().minute / 10)

        while (True):
            minute_now = int(datetime.datetime.now().minute / 10)
            if (minute_now == last_min):
                time.sleep(1)
                continue

            last_min = minute_now
            try:
                self.run()
            except Exception as err:
                self.log(self.LOGE, err)           

            self.log(self.LOGI, "sleep 1 minute to prevent keep running...")
            time.sleep(60)

    def run_by_hour(self, run_first = False, max_keep_hours = 24 * 14):
        if (run_first):
            try:
                self.run()
            except Exception as err:
                self.log(self.LOGE, err)

        while (True):
            tick_now = datetime.datetime.now()
            if (tick_now.minute != 0):
                time.sleep(1)
                continue

            try:
                self.run()
            except Exception as err:
                self.log(self.LOGE, err)           

            self.log(self.LOGI, "sleep 1 minute to prevent keep running...")
            time.sleep(60)

    def run_by_day(self, run_first = False, max_keep_hours = 24 * 14):
        if (run_first):
            try:
                self.run()
            except Exception as err:
                self.log(self.LOGE, err)

        while (True):
            tick_now = datetime.datetime.now()
            if ((tick_now.hour != 0) and (tick_now.minute != 0)):
                time.sleep(1)
                continue

            try:
                self.run()
            except Exception as err:
                self.log(self.LOGE, err)           

            self.log(self.LOGI, "sleep 1 minute to prevent keep running...")
            time.sleep(60)
