# -*- coding: utf-8 -*-
"""
Created on 2017-08-16
@author: mpdesign
"""

from core.include import *


def configdb(db_config_name='center'):
    dc = DB_CONFIG[db_config_name]
    host = dc["host"]
    user = dc["user"]
    passwd = dc["password"]
    defaultdb = dc["db"]
    port = int(dc["port"])
    return host, user, passwd, defaultdb, port


#db_con 全局数据库配置变量db_config
def db(db_type='center', app_id=''):
    if not db_type:
        db_type = 'center'
    if db_type[0:len(DB_PREFIX)] != DB_PREFIX:
        db_type = DB_PREFIX + db_type
    if app_id:
        # 未配置则查询数据库
        db_config_name = '%s_%s' % (db_type, app_id)
        # 随机更新配置
        db_rand = random.randint(1, 10)
        if db_config_name not in DB_CONFIG.keys() or db_rand < 3 :
            sql = "select * from %s where app_id='%s' and db='%s' limit 1" % (CONFIG_TABLE, app_id, db_type)
            game = db().query(sql)
            if game and isinstance(game, type({})):
                db_config_name = "%s_%s" % (game["db"], app_id)
                DB_CONFIG[db_config_name] = {"host": game["host"], "user": game["user"], "password": game["password"], "db": db_config_name, "port": game["port"]}
            else:
                output('Db config error: %s' % sql)
                return
    else:
        db_config_name = db_type
    host, user, passwd, defaultdb, port = configdb(db_config_name)
    return singleton.getinstance('mysql', 'core.db.mysql').conn(host, user, passwd, defaultdb, port)


# 安全执行，执行前检查表是否存在
def db_save(table='', data=None, conditions=None, app_id='', dbname='', tableMaxRows=0, tableFormat=''):
    if not data:
        return None
    row = data[0] if isinstance(data, type([])) else data
    if not dbname:
        output('Dbname %s is none' % dbname)
    if dbname[0:len(DB_PREFIX)] != DB_PREFIX:
        dbname = DB_PREFIX + dbname
    # 概率检查表记录数是否超过限制
    if tableMaxRows > 0:
        table = checkTable(app_id, {"table": table, "tableMaxRows": tableMaxRows, "dbname": dbname})
    # 按当前数据的时间分表
    elif tableFormat:
        v_time = time() if 'v_time' not in row else row['v_time']
        table = "%s:%s" % (table, time.strftime(tableFormat, time.localtime(v_time)))
    # database.table
    tname = "%s" % table
    reslut = db(dbname, app_id).save(table=tname, data=data, conditions=conditions)
    # 表不存在，则创建
    if str(reslut) == '1146' or str(reslut).find("doesn't exist") > 0:
        tableindb(app_id, dbname=dbname, table=table)
        reslut = db(dbname, app_id).save(table=tname, data=data, conditions=conditions)
    # 按天动态分区，不存在则新增
    if str(reslut) == '1526' or str(reslut).find("Table has no partition") > 0:

        partitionintableforday(app_id, dbname, table, v_day=itemDict(row, 'v_day'))
        reslut = db(dbname, app_id).save(table=tname, data=data, conditions=conditions)
    return reslut


# 保存元数据
def db_save_data(table='', data=None, conditions=None, app_id=''):

    # 通过主键ID更新
    if conditions and len(conditions) > 0:
        save_id = db('data', app_id).find(table=table, conditions=conditions, limit='1', fields=['id'])
        if save_id and isinstance(save_id, type({})) and 'id' in save_id.keys():
            save_id = save_id['id']
            return db_save(table, data, {"id": save_id}, app_id, dbname='data')
    # 插入数据
    else:
        # 玩家ID唯一
        if table in ['d_player', 'd_user']:
            if not isinstance(data, type([])):
                data = [data]
            for d in data:
                db_save(table, d, conditions, app_id, dbname='data')
        else:
            # 批量更新, 按年分表
            return db_save(table, data, conditions, app_id, dbname='data', tableFormat='%Y')


# 保存标签数据
def db_save_tag(table='', data=None, conditions=None, app_id=''):
    return db_save(table, data, conditions, app_id, dbname='tag')


# 保存reporter数据，按数据量分表，按平台OS分区
def db_save_reporter(table='', data=None, conditions=None, app_id=''):
    return db_save(table, data, conditions, app_id, dbname='reporter', tableMaxRows=50000000)


# 检测表是否超过记录数
def checkTable(app_id, schemes):
    table = schemes['table']
    dbname = schemes['dbname']
    if dbname[0:len(DB_PREFIX)] != DB_PREFIX:
        dbname = schemes['dbname'] = DB_PREFIX + dbname
    # 按数据量分表
    tableMaxRows = schemes['tableMaxRows'] if 'tableMaxRows' in schemes.keys() and intval(schemes['tableMaxRows']) > 0 else 5000000
    if tableMaxRows > 0:
        # 每天凌晨2-6点高概率性检测，白天概率检测：中，晚上概率检测：低
        h = int(time.strftime('%H', time.localtime()))
        if 2 <= h <= 6:
            tableRand = 1
        elif 7 <= h <= 18:
            tableRand = 0.1
        else:
            tableRand = 0.05
        r = random.randint(1, 1000)
        if 0 <= r <= 100*tableRand:
            last_id = db(dbname, app_id=app_id).debug(mode=False, code=[1146]).query("select id from %s order by id desc limit 1" % table)
            if not emptyquery(last_id) and intval(last_id['id']) >= tableMaxRows:
                newtable = "%s:%s" % (table, time.strftime('%Y%m%d%H', time.localtime()))
                renameTable(app_id, dbname, table, newtable)
    return table


# 重命名新表
def renameTable(app_id='', dbname='', table='', newtable=''):
    if dbname[0:len(DB_PREFIX)] != DB_PREFIX:
        dbname = DB_PREFIX + dbname
    # 判断新表是否已存在
    dbname = "%s_%s" % (dbname, app_id)
    tableexists = db(dbname, app_id=app_id).query("SELECT * FROM information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (dbname, newtable))
    if emptyquery(tableexists):
        db(dbname, app_id=app_id).execute("alter table %s rename to %s" % (table, newtable))
        return True
    return False


# 判断表是否存在，不存在则从模板库拷贝
def tableindb(app_id='', dbname='data', table=''):
    if not app_id:
        return False
    if dbname[0:len(DB_PREFIX)] != DB_PREFIX:
        dbname = DB_PREFIX + dbname
    tableexists = db(dbname, app_id=app_id).query("SELECT * FROM information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % ("%s_%s" % (dbname, app_id), table))
    if emptyquery(tableexists):
        tname = "%st_%s" % (DB_PREFIX, table)
        #根据模板表建表时,模板表的表名应去掉按年分表标识
        t_tname = tname.split(':')[0]
        table_struct = db(DB_PREFIX + 'table_template').query("SHOW CREATE TABLE %s" % t_tname)

        create_table = table_struct['Create Table'].replace(t_tname, tname).replace(DB_PREFIX + 't_', '')
        db(dbname, app_id).execute(create_table)
    return True


# 判断LIST天分区是否存在，不存在则新增, partiton d{v_day} BY LIST(v_day)
def partitionintableforday(app_id='', dbname='', table='', v_day=0):
    v_day = int(time.strftime('%Y%m%d', time.localtime())) if v_day < 1 else v_day
    # 是否存在按天分区
    partition_today = db(dbname, app_id=app_id).query("select * from information_schema.partitions  where table_schema='%s' and table_name='%s' and PARTITION_METHOD='LIST' order by partition_name desc limit 1" % ("%s_%s" % (dbname, app_id), table))
    if not emptyquery(partition_today) and partition_today['PARTITION_NAME'][0:1] == 'd':
        # 判断今日是否已分区
        if intval(partition_today['PARTITION_NAME'][1:]) != v_day:
            db(dbname, app_id=app_id).query("ALTER TABLE `%s` ADD PARTITION (PARTITION d%s VALUES IN (%s))" % (table, v_day, v_day))
    return True


# 获取分表列表 按时间升序
def subTableList(app_id, table='d_record', order='desc', start_day=0, end_day=0, db_type=DB_PREFIX + 'data'):
    tables = db(db_type, app_id=app_id).query("SELECT * FROM information_schema.TABLES where TABLE_SCHEMA='%s_%s' and TABLE_NAME like '%s_%s'" % (db_type, app_id, table, '%'), "all")
    table = list()
    if not emptyquery(tables):
        table_len = len(tables)

        days = []
        for j in range(0, table_len):
            tnamesp = tables[j]['TABLE_NAME'].split("/")
            days.append(intval(tnamesp[-1:][0]))
        # 排序
        k = table_len
        for i in range(0, table_len):
            for j in range(0, k):
                if j >= k - 1:
                    continue
                orderType = days[j] > days[j + 1] if order == 'asc' else days[j] < days[j + 1]
                if orderType:
                    tmp = days[j]
                    days[j] = days[j + 1]
                    days[j + 1] = tmp
            k -= 1
        # 定位时间范围
        if start_day > 0 and end_day > 0:
            # v_day = int(time.strftime('%Y%m%d', time.localtime()))
            for i in range(0, table_len):
                this_day = days[i]
                prev_day = days[i - 1] if i - 1 >= 0 else 0
                next_day = days[i + 1] if i + 1 < table_len else 0
                pn_day = prev_day if order == 'asc' else next_day
                if start_day <= this_day <= end_day or pn_day <= end_day <= this_day:
                    table.append("%s_%s" % (table, days[i]))
            else:
                if order == 'asc' and (end_day >= days[table_len - 1] or start_day >= days[table_len - 1]):
                    table.append(table)
                elif order != 'asc' and (end_day >= days[0] or start_day >= days[0]):
                    table = [table] + table
        else:
            table.append(table)
            for d in days:
                table.append("%s_%s" % (table, d))
    else:
        table.append(table)
    return table