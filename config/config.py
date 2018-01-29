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
    "tmp_path": "/tmp",
    "log_path": "/tmp/logs",
    "hdfs_home": "/gflow"
}

DB_PREFIX = 'xy_'
CONFIG_TABLE = 'xy_db'
GAME_TABLE_NAME = 'xy_game'

# 数据库配置
DB_CONFIG = {
    "xy_center": {
        "host": "192.168.1.186", "user": "public", "password": "public", "db": "xy_center", "port": 3306
    },
    "xy_table_template": {
         "host": "192.168.1.186", "user": "public", "password": "public", "db": "xy_table_template", "port": 3306
    },
    "xy_adtrack": {
        "host": "192.168.1.186", "user": "public", "password": "public", "db": "xy_adtrack", "port": 3306
    }
}

# redis配置
REDIS_CONFIG = {
    "xy_data": {
        "host": "192.168.1.182", "port": 6379, "db": 0
    },
    "xy_cache": {
        "host": "192.168.1.182", "port": 6379, "db": 1
    },
    "xy_online": {
        "host": "192.168.1.182", "port": 6379, "db": 2
    },
    "xy_device": {
        "host": "192.168.1.182", "port": 6379, "db": 3
    },
    "xy_base_lua": {
        "host": "192.168.1.182", "port": 6379, "db": 4
    }
}

# maxminddb路径配置
MAXMIND_DB_CONFIG = {
    "path_city": PATH_CONFIG["project_path"]+'/exts/GeoLite2-City.mmdb'
}
DEFAULT_DB = DB_CONFIG['xy_center']
DEFAULT_REDIS = REDIS_CONFIG['xy_data']

# HDFS
HDFS_CONFIG = {
    "host": "192.168.1.195",
    "webport": 50070,
    "port": 9000
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
DEFAULT_NODE = '172.18.0.101'

# master node
MASTER_NODE = "172.18.0.101"

# slave node
SLAVE_NODE = []

# 游戏ID范围, 1,10 表示游戏ID1-10的游戏
APP_ID_RANGE = '20001,'