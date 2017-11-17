# -*- coding: utf-8 -*-
# Filename: config.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-07-11
# Author:       mpdesign
# description:  配置脚本
# -----------------------------------

# 当前任务脚本路径
PATH_CONFIG = {
    "project_path": "/data/nebula/analysis/gflow",
    "log_path": "/data/nebula/analysis/gflow/tmp/logs"
}

# 数据库配置
DB_CONFIG = {
    "ga_center": {
        "host": "192.168.1.186", "user": "public", "password": "public", "db": "ga_center", "port": 3306
    },
    "ga_adtrack": {
        "host": "192.168.1.186", "user": "public", "password": "public", "db": "ga_adtrack", "port": 3306
    }
}

# redis配置
REDIS_CONFIG = {
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
    # 系统使用
    "sys": {
        "host": "192.168.1.182", "port": 6379, "db": 0
    }
}

# 短信配置
SMS_CONFIG = {"sdk": "13779953612", "code": "Youli888888", "subcode": "2278", "to": ["15880215195", "18850580928"]}

# 邮箱配置
MAIL_CONFIG = {"host": "smtp.exmail.qq.com",
               "user": "service@qyy.com",
               "password": "Qyy2012)@!$",
               "name": "数据中心警报",
               "to": ["84086365@qq.com"]}

# ip127.0.0.1 is not allowed to be set to the master or slave node
# 默认运算节点
DEFAULT_NODE = '172.18.0.5'

# master node
MASTER_NODE = {"ip": "172.18.0.5"}

# slave node
SLAVE_NODE = [{"ip": "172.18.0.5"}]

# 内网地址规则
LAN_IP_PREFIX = '172.18.'

# 游戏ID范围, 1,10 表示游戏ID1-10的游戏
APP_ID_RANGE = '10002,'

# 调试模式
DEBUG_MODE = 1