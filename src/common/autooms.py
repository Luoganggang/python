#!/usr/bin/python3
#-*-coding:utf-8-*-

import time
from abc import ABCMeta, abstractmethod

class Base(object):
    LOGE = 0
    LOGW = 1
    LOGI = 2    
    LOGD = 3
    LOG_LEVEL_MAP = {LOGE:"E", LOGW:"W",  LOGI:"I", LOGD:"D"}
    
    log_level = LOGD
    log_tag = "Base"
    
    logdbconn = None
    logdbcursor = None
    #logmutex = threading.Lock()

    def __init__(self):
        super(Base,  self).__init__()
        self.set_log_level(self.LOGI)
        #self.connect_log_db()

    def set_log_tag(self,  tag):
        self.log_tag = tag
        
    def set_log_level(self, level):
        self.log_level = level
        
    def log(self, level, msg):
        if (level <= self.log_level):
            tt = time.time()
            t = time.localtime(tt)
            #log = log.decode("utf-8")
            time_str = "%d-%02d-%02d %02d:%02d:%02d.%s" % \
                (t.tm_year,  t.tm_mon,  t.tm_mday,  t.tm_hour,  t.tm_min,  t.tm_sec,  ("%.4f" % (tt - int(tt))).replace("0.", "") )
            print ("[%s] [%s] [%s] %s" % (time_str,  self.LOG_LEVEL_MAP[level], self.log_tag,  msg))
            
            if (self.logdbconn != None):
                sql = "insert into logs (tag, datetime, log) values (%s, %s, %s);"
                self.logmutex.acquire()
                self.logdbcursor.execute(sql,  (self.log_tag,  time_str,  msg))
                self.logdbconn.commit()
                self.logmutex.release()

    def logtag(self, level, tag, msg):
        if (level <= self.log_level):
            tt = time.time()
            t = time.localtime(tt)
            #log = log.decode("utf-8")
            time_str = "%d-%02d-%02d %02d:%02d:%02d.%s" % \
                (t.tm_year,  t.tm_mon,  t.tm_mday,  t.tm_hour,  t.tm_min,  t.tm_sec,  ("%.4f" % (tt - int(tt))).replace("0.", "") )

            print ("[%s] [%s] [%s] %s" % (time_str, self.LOG_LEVEL_MAP[level], tag, msg))
            
            if (self.logdbconn != None):
                sql = "insert into logs (tag, datetime, log) values ('%s', '%s', '%s');" 
                self.logmutex.acquire()
                self.logdbcursor.execute(sql,  (tag,  time_str,  msg))
                self.logdbconn.commit()
                self.logmutex.release()

