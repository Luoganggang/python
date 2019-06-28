#!/usr/bin/python3
#-*-coding:utf-8-*-

# !!!注意：本机需要与10.0.16.69提前建立ssh免密码登录：
# 本机上运行：
# cd ~/.ssh
# ssh-keygen -t rsa
# ssh-copy-id -i ./id_rsa viewer@10.0.16.69
# 输入密码，然后 ssh viewer@10.0.16.69测试可以免密码登录

import sys
import os
sys.path.append('../common')
from service import *

import time
import datetime

class AppMarketError(QueryService):
    map_operate_types = {"1":"BT下载失败", "2":"HTTP下载失败", "3":"安装失败", "4":"HTTP请求失败"}
    map_error_codes = {"21":"启动BT下载失败",
                    "22":"BT下载url无效",
                    "23":"BT下载client初始化失败",
                    "24":"BT下载Announce失败",
                    "25":"BT下载任务未完成",
                    "41":"URL不合法",
                    "42":"网络拒绝访问",
                    "43":"网络超时",
                    "44":"文件不存在",
                    "45":"文件拒绝访问",
                    "46":"存储空间不足",
                    "47":"检测下载文件失败",
                    "1002":"连接错误",
                    "1003":"SocketTimeout",
                    "3000":"相应内容为空",
                    "3001":"格式转换出错",
                    "-7777":"应用市场需要更新",
                    "-8888":"设备维护中",
                    "-9999":"会话失效",
                    "-1":"已经安装过",
                    "-2":"安装包无效",
                    "-3":"安装apk所在URI无效",
                    "-4":"存储空间不足",
                    "-5":"已经存在同包名应用",
                    "-6":"共享用户不存在",
                    "-7":"安装的包有不同的签名相同的名称比新包(旧的包的数据并没有删除)。",
                    "-8":"新包请求一个共享的用户已安装在设备上,没有匹配的签名。",
                    "-9":"新的包使用一个共享库,是不可用的。",
                    "-10":"新的包使用一个共享库,是不可用的。",
                    "-11":"优化和验证其敏捷的文件,因为没有足够的存储或验证失败。",
                    "-12":"SDK大于Android设备运行安装的apk版本",
                    "-13":"包含一个内容提供者提供者一样的权威已经安装在系统中。",
                    "-14":"当前设备SDK版本高于安装包所运行的版本",
                    "-15":"测试包或者手机不允许安装测试包",
                    "-16":"so库，与设备的CPU_ABI不兼容。",
                    "-17":"设备上不支持此包的新的特性",
                    "-18":"一个安全容器挂载点无法访问外部媒体",
                    "-19":"包不能被安装在指定的安装位置。",
                    "-20":"新包不能被安装在指定的安装位置,因为媒体是不可用的。",
                    "-21":"无法安装新的包,因为验证超时。",
                    "-22":"验证不成功",
                    "-23":"预期的应用被改变",
                    "-24":"新的包分配一个与过去不同的UID。",
                    "-25":"当前程序的版本低于已安装的程序",
                    "-26":"应用已经安装，覆盖安装发现编译版本低于已经安装的",
                    "-100":"安装文件不是文件或者不是以.apk结尾",
                    "-101":"解析失败，无法提取Manifest",
                    "-102":"解析失败，无法预期的异常",
                    "-103":"解析失败，找不到证书",
                    "-104":"解析失败，证书不一致",
                    "-105":"解析失败，证书编码异常",
                    "-106":"解析失败，manifest中的包名错误或丢失",
                    "-107":"解析失败，manifest中的共享用户错误",
                    "-108":"解析失败，manifest中出现结构性错误",
                    "-109":"解析器并没有发现任何可行的标签(Application应用程序)的清单。",
                    "-110":"系统问题导致安装失败",
                    "-111":"系统没有安装包,因为用户限制安装应用程序。",
                    "-112":"已经安装的APK使用了某个服务自定义的Service权限，安装使用相同服务不同签名的应用",
                    "-113":"是由于使用了nativelibraries，该nativelibraries不支持当前的cpu的体系结构。",
                    "-114":"NativeLibraryHelper方法的内部返回码用于指示正在处理的包不包含任何本地代码。",
                    "-115":"安装被终止",
                    # 新加的不能解析的错误码
                    "0":"下载失败，详见stackInfo",
                    "1001":"HTTP请求失败，详见stackInfo",
                    "2":"安装失败",
                    "500":"HTTP请求失败500-Internal Server Error",
                    "502":"HTTP请求失败502-Bad Gateway",
                    "503":"HTTP请求失败503-Service Unavailable"}

    def __init__(self):
        super(QueryService,  self).__init__()
        self.set_log_level(self.LOGD)
        self.set_log_tag("AppMarketError")

    def prepare(self):
        DataImporter.prepare(self)
        self.log(self.LOGD, "connecting to info db...")
        if (self.connect_info_db() == False):
            return False

        return True
       
    def execute(self):
        # 等待服务器一段时间生成日志文件
        time.sleep(60)

        tick_now = datetime.datetime.now()
        tick_now = tick_now.replace(microsecond=0,second=0)
        str_minute_now = tick_now.strftime('%Y-%m-%d %H:%M')

        tick_minute_ago = tick_now - datetime.timedelta(minutes = 1)
        str_minute_ago = tick_minute_ago.strftime('%Y-%m-%d %H:%M')     
        #print (str_minute_now)
        #print (str_minute_ago)

        # 复制服务器日志文件到本地
        now_minute = int(datetime.datetime.now().minute / 10) * 10
        server_filename = "market_fail_%s" % (datetime.datetime.now().replace(minute=now_minute).strftime("%Y%m%d%H%M"))
        local_filename = "./lasterror.txt"
        self.log(self.LOGD, "copy file from server..." + server_filename)
        #print (server_filename)

        os.system("rm -f " + local_filename)
        os.system("scp viewer@10.0.16.69:/data/yw_opert/app_fail/log/%s %s" % (server_filename, local_filename))

        if (os.path.exists(local_filename) == False):
            self.log(self.LOGI, "local file not exits")
            return True

        f = open(local_filename, "r")
        logs = f.readlines()
        f.close()

        self.log(self.LOGD, "processing logs, total %d " % len(logs))
        for log in logs:
            if (self.process_log(log) == False):
                return False

        return True

    def get_idc_name(self, vm_ip):
        sql = "select idc_name from rf_pad inner join rf_idc on rf_idc.idc_id=rf_pad.idc_id where pad_ip='%s'" % vm_ip
        self.read_cursor_dict_info.execute(sql)
        rows2 = self.read_cursor_dict_info.fetchall()
        if (len(rows2) == 0):
            return "未知机房"
        if (len(rows2[0]) == 0):
            return "未知机房"

        return rows2[0][0]
               
    def process_log(self, log):
        start_pos = log.find(",")
        if (start_pos == -1):
            self.log(self.LOGI, "cannot find , skipped log")
            return True

        # 去掉前缀
        real_log = log[start_pos + 1:]
        #print (real_log)

        # 提取stackInfo
        stack_info_pos = real_log.find("stackInfo=")
        if (stack_info_pos == -1):
            self.log(self.LOGI, "cannot find stackInfo skipped log")
            return True

        stack_info = real_log[stack_info_pos + len("stackInfo="):]
        #print (stack_info)

        real_log = real_log[:stack_info_pos]
        #print (real_log)

        log_items = real_log.split(",")
        #print (log_items)

        # key:value
        map_logs = {}
        for log_item in log_items:
            if (log_item == ""):
                continue

            line = log_item.split("=")
            if (len(line) < 2):
                line_value = "null"
            else:
                line_value = line[1]

            map_logs[line[0]] = line_value

        # 转换操作类型和错误码
        map_logs['errorType'] = 'fail'
        if (map_logs['operateType'] in self.map_operate_types):
            map_logs['operateType'] = '%s [%s]' % (self.map_operate_types[map_logs['operateType']], map_logs['operateType'])
        else:
            map_logs['operateType'] = '未知 [%s]' % (map_logs['operateType'])

        if (map_logs['errorCode'] in self.map_error_codes):
            map_logs['errorCode'] = '%s [%s]' % (self.map_error_codes[map_logs['errorCode']], map_logs['errorCode'])
        else:
            map_logs['errorCode'] = '未知 [%s]' % (map_logs['errorCode'])

        map_logs['idc'] = self.get_idc_name(map_logs['ip'])
        map_logs['stackInfo'] = stack_info
        #print (map_logs)

        # 将此条日志插入到数据库
        return self.insert_log(map_logs)

    def insert_log(self, map_logs):
        # 导入到数据库
        sql = "INSERT INTO service_app_market_error "
        sql = sql + "(log_time, error_type, operate_type, vm_ip, idc, "
        sql = sql + " os_type, market_version, device_version, game_id, "
        sql = sql + " package_name, app_name, error_code, api_code, stack_info) "
        sql = sql + " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "

        if (map_logs['stackInfo'].find("name:tun0") >= 0):
            have_vpn = True
        else:
            have_vpn = False

        parameters = []
        parameters.append(map_logs['operateDate'])                   
        parameters.append(map_logs['errorType'])                   

        if (have_vpn):
            parameters.append("VPN " + map_logs['operateType'])
        else:
            parameters.append(map_logs['operateType'])

        parameters.append(map_logs['ip'])
        parameters.append(map_logs['idc'])
        parameters.append(map_logs['osType'])
        parameters.append(map_logs['marketVersion'])
        parameters.append(map_logs['deviceVersion'])
        parameters.append(map_logs['gameId'])
        parameters.append(map_logs['packageName'])
        parameters.append(map_logs['appName'])

        if (have_vpn):
            parameters.append("VPN " + map_logs['errorCode'])
        else:
            parameters.append(map_logs['errorCode'])

        parameters.append(map_logs['apiCode'])
        parameters.append(map_logs['stackInfo'])

        self.write_cursor.execute(sql, parameters)
        self.write_conn.commit()
        return True

if __name__ == "__main__":
    print ("App Market Fail V1.0")
    print ("(C) 2019 Guangzhou MC Information Technology Co., Ltd.")
    print ("Authors: Shi Qizheng (shiqizheng@armvm.cn)")
    obj = AppMarketError()
    obj.run_by_10min()
    
