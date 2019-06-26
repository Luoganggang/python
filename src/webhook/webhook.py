#!/usr/bin/python3
#-*-coding:utf-8-*-
import tornado.ioloop
import tornado.web
import os
import datetime
import sys

sys.path.append('../common')
#from network import *

sys.path.append('../dataimporters/common')
from dataimporters import *

class ErrorQuality(DataImporter):
    def __init__(self):
        super(ErrorQuality,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("ErrorQuality")

    def set_data(self, source, destination, carrier, message):
        self.source = source
        self.destination = destination
        self.carrier = carrier
        self.message = message

    def execute(self):
        tick_now = datetime.datetime.now()

        # 写入到数据库
        self.write_cursor.execute('INSERT INTO network_error_by_quality (log_time, source, dest, carrier, message) VALUES (%s, %s, %s, %s, %s)', (tick_now, self.source, self.destination, self.carrier, self.message))

        self.write_conn.commit()

"""
data={'source':idc,'destination':Destination_Idc,'carrier':Link_Type,'alert':alertname}
"""
"""
http://localhost:3001/addnetworkevent?source=番禺&destination=上海&carrier=电信出口&alert=延时过大，如果此提示连续出现2次以上，请登录WEB确认网络质量
"""
class HandlerAddNetworkEvent(tornado.web.RequestHandler):
    def get(self):
        print (datetime.datetime.now())
        source = self.get_argument("source")
        destination = self.get_argument("destination")
        carrier = self.get_argument("carrier")
        alert = self.get_argument("alert")

        obj = ErrorQuality()
        obj.set_data(source, destination, carrier, alert)
        obj.run()

        self.write("OK")


"""
http://localhost:3001/addrecycleevent?recycle_type=1&total_input=500&total_ok=100&total_error=200&total_seconds=300
"""
class DeviceRecycle(DataImporter):
    def __init__(self):
        super(DeviceRecycle,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("DeviceRecycle")

    def set_data(self, data):
        self.recycle_type = data[0]
        self.total_input = data[1]
        self.total_ok = data[2]
        self.total_error = data[3]
        self.total_seconds = data[4]

    def execute(self):
        tick_now = datetime.datetime.now()

        # 写入到数据库
        self.write_cursor.execute('INSERT INTO device_recycle (log_time, recycle_type, total_input, total_ok, total_error, total_seconds) VALUES (%s, %s, %s, %s, %s, %s)', (tick_now, self.recycle_type, self.total_input, self.total_ok, self.total_error, self.total_seconds))

        self.write_conn.commit()

class HandlerAddRecycleEvent(tornado.web.RequestHandler):
    def get(self):
        recycle_type = self.get_argument("recycle_type")
        total_input = self.get_argument("total_input")
        total_ok = self.get_argument("total_ok")
        total_error = self.get_argument("total_error")
        total_seconds = self.get_argument("total_seconds")

        print (recycle_type, total_input, total_ok, total_error, total_seconds)

        obj = DeviceRecycle()
        obj.set_data((recycle_type, total_input, total_ok, total_error, total_seconds))
        obj.run()

        self.write("OK")

settings = {
    "static_path": os.path.join(os.path.dirname(__file__), "static") 
}

application = tornado.web.Application([
    (r"/addnetworkevent", HandlerAddNetworkEvent),
    (r"/addrecycleevent", HandlerAddRecycleEvent),
], **settings)

if __name__ == "__main__":
    print ("Event Receiver V1.0")
    print ("Copyright(C) 2019 Hunan MC Information Co., Ltd.")
    print ("http://www.armvm.cn")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    print ("")

    print (datetime.datetime.now())
    print ("Starting server...")
    listen_port = 3001
    application.listen(listen_port)
    print ("Web hook server is ready, waiting for clients on port %d" % listen_port)
    tornado.ioloop.IOLoop.instance().start()
