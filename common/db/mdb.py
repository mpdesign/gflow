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
def memory(redis_config_name='ga_data'):
    if redis_config_name not in redis_config.keys():
        redis_config_name = 'ga_data'
    rc = redis_config[redis_config_name]
    host = rc["host"]
    port = rc["port"]
    dbno = rc["db"]
    return singleton.getinstance('redisdb', 'core.db.redisdb').conn(host, port, dbno)


# 配置redis
def redisConfig(redis_type='ga_data', app_id=''):
    _type = redis_type[3:] if redis_type[0:3] == 'ga_' else redis_type
    redis_config_name = "%s_%s" % (_type, app_id)
    if app_id:
        # 未配置则查询数据库
        # 随机更新配置
        redis_rand = random.randint(1, 10)
        if redis_config_name not in redis_config.keys() or redis_rand < 3:
            # 缓存配置
            redis_config_key = "ga_redis_%s" % redis_config_name
            result = memory(redis_config_name='ga_cache').get(redis_config_key, j=True)
            if not result:
                sql = "select * from ga_db where app_id='%s' and db='redis_%s' limit 1" % (app_id, _type)
                result = db().query(sql)
                if not emptyquery(result):
                    r = {"host": result["host"], "port": result["port"], "db": result["user"]}
                    redis_config[redis_config_name] = r.copy()
                else:
                    r = '-1'
                memory(redis_config_name='ga_cache').set(redis_config_key, r, 300, j=True)
            elif result != '-1':
                redis_config[redis_config_name] = result

    redis_config_name = redis_type if redis_config_name not in redis_config.keys() else redis_config_name
    return redis_config_name


# 获取表索引
def get_index(app_id, dbname, tablename):
    index_sql = "select TABLE_NAME,group_concat(COLUMN_NAME) as COLUMN_NAME from information_schema.STATISTICS WHERE TABLE_SCHEMA = '%s' and TABLE_NAME ='%s' group by TABLE_NAME" % (dbname, tablename)
    result = db(db_type='ga_reporter', app_id=app_id).query(index_sql)
    if not emptyquery(result):
        indexname = result['COLUMN_NAME']
        return indexname


# 获取表字段并排序
def get_fields(row):
    fields = []
    for k in row:
        fields.append(k)
    return sorted(fields)