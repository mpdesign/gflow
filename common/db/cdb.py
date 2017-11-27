# -*- coding: utf-8 -*-
"""
Created on 2017-08-16
@author: mpdesign
"""

from core.include import *


def configdb(db_config_name=CENTER_NAME):
    dc = DB_CONFIG[db_config_name]
    host = dc["host"]
    user = dc["user"]
    passwd = dc["password"]
    defaultdb = dc["db"]
    port = int(dc["port"])
    return host, user, passwd, defaultdb, port


#db_con 全局数据库配置变量db_config
def db(db_type=CENTER_NAME, app_id=''):
    if app_id:
        if db_type:
            if db_type[0:len(PREFIX_NAME)] != PREFIX_NAME:
                db_type = PREFIX_NAME + db_type
            # 未配置则查询数据库
            db_config_name = '%s_%s' % (db_type, app_id)
            # 随机更新配置
            db_rand = random.randint(1, 10)
            if db_config_name not in DB_CONFIG.keys() or db_rand < 3 :
                sql = "select * from %s where app_id='%s' and db='%s' limit 1" % (DB_TABLE_NAME, app_id, db_type)
                game = db().query(sql)
                if game and isinstance(game, type({})):
                    db_config_name = "%s_%s" % (game["db"], app_id)
                    DB_CONFIG[db_config_name] = {"host": game["host"], "user": game["user"], "password": game["password"], "db": db_config_name, "port": game["port"]}
                else:
                    output('Db config error: %s' % sql)
                    return
        else:
            output('Db config name error: db_type[%s] app_id[%s] ' % (db_type, app_id))
            return
    elif db_type not in DB_CONFIG.keys():
        db_config_name = CENTER_NAME
    else:
        db_config_name = db_type
    host, user, passwd, defaultdb, port = configdb(db_config_name)
    return singleton.getinstance('mysql', 'core.db.mysql').conn(host, user, passwd, defaultdb, port)


# 安全执行，执行前检查表是否存在
def db_save(table='', data=None, conditions=None, app_id='', dbname=''):
    if not dbname:
        output('Dbname %s is none' % dbname)
    if dbname[0:len(PREFIX_NAME)] != PREFIX_NAME:
        dbname = PREFIX_NAME + dbname
    reslut = db(dbname, app_id).save(table=table, data=data, conditions=conditions)
    if str(reslut) == '1146' or str(reslut).find("doesn't exist") > 0:
        tableindb(table, app_id, dbname=dbname)
        # database.table
        tname = "%s_%s.%s" % (dbname, app_id, table)
        reslut = db(dbname, app_id).save(table=tname, data=data, conditions=conditions)
    return reslut


# 保存元数据
def db_save_data(table='', data=None, conditions=None, app_id='', check=True, tableMaxNum=10000000):
    if not data:
        return None
    # 检查最新可用表
    if check:
        # 游戏记录表， 采用ARCHIVE引擎，按年分表、按日list动态分区（判断是否存在）
        table = checkTable(app_id, {"table": table, "tableMaxNum": tableMaxNum, "dbname": PREFIX_NAME + 'data'})
    # 通过主键ID更新
    if conditions and len(conditions) > 0:
        save_id = db(PREFIX_NAME + 'data', app_id).find(table=table, conditions=conditions, limit='1', fields=['id'])
        if save_id and isinstance(save_id, type({})) and 'id' in save_id.keys():
            save_id = save_id['id']
            return db_save(table, data, {"id": save_id}, app_id, dbname=PREFIX_NAME + 'data')
    # 插入数据
    else:
        # 玩家ID唯一
        if table in ['d_player', 'd_user']:
            if not isinstance(data, type([])):
                data = [data]
            for d in data:
                db_save(table, d, conditions, app_id, dbname=PREFIX_NAME + 'data')
        else:
            # 批量更新
            return db_save(table, data, conditions, app_id, dbname=PREFIX_NAME + 'data')


# 保存标签数据
def db_save_tag(table='', data=None, conditions=None, app_id='', check=True, tableMaxNum=1000000):
    # 检查最新可用表
    if check:
        table = checkTable(app_id, {"table": table, "tableMaxNum": tableMaxNum, "dbname": PREFIX_NAME + 'tag'})
    return db_save(table, data, conditions, app_id, dbname=PREFIX_NAME + 'tag')


# 保存reporter数据
def db_save_reporter(table='', data=None, conditions=None, app_id='', check=True, tableMaxNum=1000000):
    # 检查最新可用表
    if check:
        # 对平台字段os进行分区，预计分5区，按数据量分表
        table = checkTable(app_id, {"table": table, "tableMaxNum": tableMaxNum, "dbname": PREFIX_NAME + 'reporter'})
    return db_save(table, data, conditions, app_id, dbname=PREFIX_NAME + 'reporter')


# 判断表是否存在，不存在则从主库拷贝
def tableindb(table, app_id='', dbname=PREFIX_NAME + 'data'):
    if not app_id:
        return False
    table_exits = False
    tables = db(dbname, app_id).query("SHOW TABLES", "all")
    if tables:
        tindb = 'Tables_in_%s_%s' % (dbname, app_id)
        for t in tables:
            if tindb in t.keys() and t[tindb] == table:
                table_exits = True
                break
    if not table_exits:
        tname = "t_%s" % table
        table_struct = db(PREFIX_NAME + 'table_template').query("SHOW CREATE TABLE %s" % tname)
        create_table = table_struct['Create Table'].replace('t_', '')
        db(dbname, app_id).execute(create_table)


# 获取分表列表 按时间升序
def subTable(app_id, tablename='d_record', order='desc', start_day=0, end_day=0, db_type=PREFIX_NAME + 'data'):
    tables = db(db_type, app_id=app_id).query("SELECT * FROM information_schema.TABLES where TABLE_SCHEMA='%s_%s' and TABLE_NAME like '%s_%s'" % (db_type, app_id, tablename, '%'), "all")
    table = list()
    if not emptyquery(tables):
        table_len = len(tables)

        days = []
        for j in range(0, table_len):
            days.append(intval(tables[j]['TABLE_NAME'].split("_")[2]))
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
                    table.append("%s_%s" % (tablename, days[i]))
            else:
                if order == 'asc' and (end_day >= days[table_len - 1] or start_day >= days[table_len - 1]):
                    table.append(tablename)
                elif order != 'asc' and (end_day >= days[0] or start_day >= days[0]):
                    table = [tablename] + table
        else:
            table.append(tablename)
            for d in days:
                table.append("%s_%s" % (tablename, d))
    else:
        table.append(tablename)
    return table


# 检测表是否超过记录数
def checkTable(app_id, schemes):
    tablename = schemes['table']
    # 表记录数超过1千万则分表，每天凌晨2-6点高概率性检测，白天概率检测：中，晚上概率检测：低
    h = int(time.strftime('%H', time.localtime()))
    if 2 <= h <= 6:
        tableRand = 1
    elif 7 <= h <= 18:
        tableRand = 0.1
    else:
        tableRand = 0.05
    r = random.randint(1, 1000)
    if 0 <= r <= 100*tableRand:
        tablename = renameTable(app_id, schemes)
    return tablename


# 重命名新表
def renameTable(app_id, schemes):
    dbname = schemes['dbname']
    tablename = schemes['table']
    tableMaxNum = schemes['tableMaxNum'] if 'tableMaxNum' in schemes.keys() and intval(schemes['tableMaxNum']) > 0 else 5000000
    last_id = db(dbname, app_id=app_id).debug(mode=False, code=[1146]).query("select id from %s order by id desc limit 1" % tablename)
    if not emptyquery(last_id) and intval(last_id['id']) >= tableMaxNum:
        # 当日表名
        tableFormat = schemes['tableFormat'] if 'tableFormat' in schemes.keys() else '%Y%m%d'
        newtable = "%s_%s" % (schemes['table'], time.strftime(tableFormat, time.localtime()))
        # 判断新表是否已存在
        dbname = "%s_%s" % (dbname, app_id)
        tableexists = db(dbname, app_id=app_id).query("SELECT * FROM information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (dbname, newtable))
        if emptyquery(tableexists):
            db(dbname, app_id=app_id).execute("alter table %s rename to %s" % (tablename, newtable))
    return tablename