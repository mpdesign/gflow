# -*- coding: utf-8 -*-
# Filename: config.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-07-11
# Author:       mpdesign
# description:  配置脚本
# -----------------------------------

# 项目信息
project_config = {"id": 1, "name": "gflow", "bin": "slave"}

# 当前任务脚本路径
path_config = {
    "gf_path": "/data/nebula/gflow",
    "log_path": "/data/nebula/gflow/logs",
    "pid_path": "/data/nebula/gflow/pids"
}

# 数据库配置
db_config = {
    "ga_center": {
        "host": "192.168.1.186", "user": "public", "password": "public", "db": "ga_center", "port": 3306
    },
    "ga_adtrack": {
        "host": "192.168.1.186", "user": "public", "password": "public", "db": "ga_adtrack", "port": 3306
    }
}

# redis配置
redis_config = {
    "ga_data": {
        "host": "192.168.1.182", "port": 6379, "db": 0
    },
    "ga_cache": {
        "host": "192.168.1.182", "port": 6379, "db": 1
    },
    "ga_online": {
        "host": "192.168.1.182", "port": 6379, "db": 2
    },
    "ga_device": {
        "host": "192.168.1.182", "port": 6379, "db": 3
    },
    "ga_db": {
        "host": "192.168.1.182", "port": 6379, "db": 4
    },
    # 系统默认
    "sys": {
        "host": "192.168.1.182", "port": 6379, "db": 0
    }
}

# 短信配置
sms_config = {"sdk": "13779953612", "code": "Youli888888", "subcode": "2278", "to": ["15880215195", "18850580928"]}

# 邮箱配置
mail_config = {"host": "smtp.exmail.qq.com",
               "user": "service@qyy.com",
               "password": "Qyy2012)@!$",
               "name": "数据中心警报",
               "to": ["84086365@qq.com"]}

# ip127.0.0.1 is not allowed to be set to the master or slave node
# master node
master_node = {"ip": "172.18.0.5"}

# slave node
slave_node = [{"ip": "172.18.0.5"}]

# 默认运算节点
default_node = '172.18.0.5'

# debug mode
# 线上请关闭
debug = False

# 内网地址规则
lan_ip_prefix = '172.18.'