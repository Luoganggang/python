#!/bin/sh
# (C) 2019 广州微算互联信息技术有限公司
# 启动与停止自动运维平台后台服务脚本
# 作者：石启铮 (shiqizheng@armvm.cn)
#

CMD=$1
ROOTDIR=`pwd`

start_dir(){
    echo "start_dir on: " $1
    cd $1
    rm -f logs.txt
    appname=${1##*/}
    nohup python3 $appname.py > logs.txt &
}

start_all(){
    echo "start_all on" $1

    # 若存在与目录名相同的PY文件，则启动该PY文件
    dirname=${1##*/}
    if [ -f "$1/$dirname.py" ]; then
        start_dir $1
    fi

    dirs=$(ls -l $1 |awk '/^d/ {print $NF}')

    for item in $dirs
    do
        if [ $item = "common" ]; then
            # 跳过common目录
            #echo "skipped common dir"
            :
        else
            # 递归搜索子目录
            start_all $1/$item
        fi
    done
}

stop_all(){
    echo "stop_all"
    killall python3
}

restart_all(){
    echo "restart_all"
    stop_all
    start_all `pwd`
}

case "$CMD" in 
startall)
    stop_all
    start_all `pwd`
	;;
stopall)
    stop_all
	;;
restart)
    restart_all
esac
