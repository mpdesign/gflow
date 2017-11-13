# -*- coding: utf-8 -*-
# Filename: py

# -----------------------------------
# Revision:     2.0
# Date:         2017-09-05
# Author:       mpdesign
# -----------------------------------

from common.common import *

# 每次取数据最大条目
max_pop_num = 1000


# 可更新字段
def mkUpdateData(row, updates):

    if 'set' in updates.keys() and 'where' in updates.keys():
        set_fields = updates["set"]
        where_fields = updates["where"]
    else:
        return None, None

    data = {}
    for key in row:
        if row[key] and key in set_fields:
            data[key] = row[key]
    if len(data) > 0:
        conditions = {}
        for wf in where_fields:
            if wf in row.keys() and row[wf]:
                conditions[wf] = row[wf]
        if conditions:
            return data, conditions
    return None, None


# 插入字段
def mkInsertData(row, fields):
    data = {}
    v_time = int(itemDict(row, 'v_time'))
    if v_time < 0:
        return None
    data['v_time'] = itemDict(row, 'v_time')
    if 'v_hour' in fields:
        row['v_hour'] = time.strftime('%Y%m%d%H', time.localtime(v_time))
    if 'v_day' in fields:
        row['v_day'] = time.strftime('%Y%m%d', time.localtime(v_time))
    if 'v_week' in fields:
        row['v_week'] = time.strftime('%Y%W', time.localtime(v_time))
    if 'v_month' in fields:
        row['v_month'] = time.strftime('%Y%m', time.localtime(v_time))

    for f in fields:
        data[f] = itemDict(row, f)
    return data

# 循环取数据
def popData(gakey, schemes):
    app_id = gakey.split('_')[-1]
    qkey = "ga_data_queue_%s" % gakey
    # 发布
    pkey = "ga_data_pub_%s" % app_id
    rows_insert = {}
    i = 0
    rows_update = {}
    while True:
        try:
            # 阻塞弹出，超时30秒,如果已有数据弹出，则不等待，继续出队，否则等待30秒出队
            if i > 0:
                r = memory(redisConfig(redis_type='ga_data', app_id=app_id)).redisInstance().lpop(qkey)
            else:
                k, r = memory(redisConfig(redis_type='ga_data', app_id=app_id)).redisInstance().blpop(qkey, timeout=30)
        except Exception, e:
            if i > 0:
                output(e, log_type='system')
                # 已获取过数据则跳出
                break
            continue
        if not r:
            if i > 0:
                # 已获取过数据则跳出
                break
            else:
                # 未取过数据，继续等待
                continue
        # 发布数据流，按需订阅
        memory(redisConfig(redis_type='ga_data', app_id=app_id)).redisInstance().publish(pkey, '%s/%s' % (schemes['table'], r))
        # json_decode
        row = singleton.getinstance('pjson').loads(r)
        if not row or 'app_id' not in row.keys() or not row['app_id']:
            continue
        app_id = row['app_id']
        if app_id not in rows_insert:
            rows_insert[app_id] = []
        if app_id not in rows_update:
            rows_update[app_id] = []
        i += 1

        for k in row:
            if not row[k]:
                row[k] = ''
            else:
                if isinstance(row[k], type('')):
                    row[k] = encode(sql_escape(row[k]))
        # 判断是否更新数据
        if 'update' in row.keys() and row['update'] and 'update' in schemes.keys():
            data, conditions = mkUpdateData(row, schemes['update'])
            if data and conditions:
                #加入更新列表
                rows_update[app_id].append((data, conditions, row))
        else:
            # 加入插入列表
            # 自定义事件
            if gakey[0:5] == 'event':
                if 'kv' in row.keys() and len(row['kv']) > 0:
                    #kvs = json.read(row['kv'])
                    for kv in row['kv']:
                        row['eventID'] = kv['eventID']
                        row['value'] = kv['value']
                        row['v_time'] = kv['v_time']
                        rows_insert[app_id].append(mkInsertData(row, schemes['fields']))
            else:
                rows_insert[app_id].append(mkInsertData(row, schemes['fields']))
        # 数据超过最大记录数则跳出
        if i > max_pop_num:
            break

    return rows_insert, rows_update

# 保存数据
def doData(rows_insert, rows_update, schemes):
    table = schemes['table']
    # 逐条更新
    if rows_update:
        for app_id in rows_update:
            for data, conditions, row in rows_update[app_id]:
                dbUpdateData(table=table, data=data, conditions=conditions, app_id=app_id, row=row, schemes=schemes)

    # 批量插入
    if rows_insert:
        for app_id in rows_insert:
            db_save_data(table=table, data=rows_insert[app_id], app_id=app_id, check=True, tableMaxNum=schemes['tableMaxNum'])


# 更新数据
def dbUpdateData(table='', data=None, conditions=None, app_id='', row={}, schemes={}):
    if not data:
        return None
    #通过主键ID更新
    if conditions and len(conditions) > 0:
        save_id = db('ga_data', app_id).find(table=table, conditions=conditions, limit='1')
        if save_id and isinstance(save_id, type({})) and 'id' in save_id.keys():
            # 更新等级的情况，等级必须大于历史等级
            if "level" in data.keys() and "level" in save_id.keys():
                level = intval(data['level'])
                lastLevel = intval(save_id['level'])
                if level <= lastLevel:
                    return None
                else:
                    # 等级level升级记录 以及 上一级lastLevel玩家等级状态：游戏时长、游戏次数、虚拟币消费、购入、充值金额
                    row['update'] = 0
                    memory(redisConfig(redis_type='ga_data', app_id=app_id)).rpush("ga_data_queue_player_level_%s" % app_id, singleton.getinstance('pjson').dumps(row))
            save_id = save_id['id']
            return db_save(table=table, data=data, app_id=app_id, conditions={"id": save_id}, dbname='ga_data')
        # 没有找到玩家则插入数据
        else:
            if table in ['d_player', 'd_user']:
                data = mkInsertData(row, schemes['fields'])
                db_save_data(table=table, data=data, app_id=app_id)