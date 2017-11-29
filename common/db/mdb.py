# -*- coding: utf-8 -*-
# Filename: memdbModel.py

# -----------------------------------
# Revision:         2.0
# Date:             2017-02-08
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      内存数据库
# -----------------------------------

from cdb import *
import json


# redis数据库实例
def memory(redis_config_name='data'):
    if redis_config_name[0:len(PREFIX_NAME)] != PREFIX_NAME:
        redis_config_name = PREFIX_NAME + redis_config_name
    if redis_config_name not in REDIS_CONFIG.keys():
        redis_config_name = PREFIX_NAME + 'data'
    rc = REDIS_CONFIG[redis_config_name]
    host = rc["host"]
    port = rc["port"]
    dbno = rc["db"]
    return singleton.getinstance('redisdb', 'core.db.redisdb').conn(host, port, dbno)


# 配置redis
def redisConfig(redis_type='data', app_id=''):
    _type = redis_type[len(PREFIX_NAME):] if redis_type[0:len(PREFIX_NAME)] == PREFIX_NAME else redis_type
    redis_config_name = "%s_%s" % (_type, app_id)
    if app_id:
        # 未配置则查询数据库
        # 随机更新配置
        redis_rand = random.randint(1, 10)
        if redis_config_name not in REDIS_CONFIG.keys() or redis_rand < 3:
            # 缓存配置
            redis_config_key = "%sredis_%s" % (PREFIX_NAME, redis_config_name)
            result = memory(redis_config_name='cache').get(redis_config_key, j=True)
            if not result:
                sql = "select * from %s where app_id='%s' and db='redis_%s' limit 1" % (DB_TABLE_NAME, app_id, _type)
                result = db().query(sql)
                if not emptyquery(result):
                    r = {"host": result["host"], "port": result["port"], "db": result["user"]}
                    REDIS_CONFIG[redis_config_name] = r.copy()
                else:
                    r = '-1'
                memory(redis_config_name='cache').set(redis_config_key, r, 300, j=True)
            elif result != '-1':
                REDIS_CONFIG[redis_config_name] = result

    redis_config_name = redis_type if redis_config_name not in REDIS_CONFIG.keys() else redis_config_name
    return redis_config_name


# 获取表字段并排序
def get_fields(row):
    fields = []
    for k in row:
        fields.append(k)
    return sorted(fields)