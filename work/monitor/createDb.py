# -*- coding: utf-8 -*-
# Filename: createDb.py

# -----------------------------------
# Revision:         2.0
# Date:             2015-04-13
# Author:           mpdesign
# description:      创建数据库
# frequency:        sleepExecute
# -----------------------------------

from monitor import *


class createDbTask(monitorJob):

    def beforeExecute(self):
        # 每分钟监听一次数据库是否已创建
        self.sleepExecute = 60

    def mapTask(self):
        return DEFAULT_NODE

    def execute(self, myTask=[]):
        # 当前可分配的数据库
        ga_admin = db().query("select * from ga_db where db='ga_admin' limit 1")
        if emptyquery(ga_admin):
            output("ga_admin db config is empty", task_name=self.__class__.__name__)
        # 创建数据库
        ga_admin['user'] = singleton.getinstance('pcode').decode(ga_admin['user']) if intval(ga_admin['user']) > 10000 else ga_admin['user']
        ga_admin['password'] = singleton.getinstance('pcode').decode(ga_admin['password']) if intval(ga_admin['password']) > 10000 else ga_admin['password']
        databases = self.conn_admin(ga_admin).query("SELECT * FROM information_schema.SCHEMATA", "all")
        if emptyquery(databases):
            output("databases is empty", task_name=self.__class__.__name__)
        existsDb = []
        for d in databases:
            existsDb.append(d["SCHEMA_NAME"])
        dbs = self.games()
        if emptyquery(dbs):
            output("has not db to be create", task_name=self.__class__.__name__)
        for d in dbs:
            # 独立分析的游戏不自动创建数据库
            if d['assign_node']:
                continue
            dataName = "ga_data_%s" % d["app_id"]
            reporterName = "ga_reporter_%s" % d["app_id"]
            tagName = "ga_tag_%s" % d["app_id"]
            configName = "ga_config_%s" % d["app_id"]
            if dataName not in existsDb:
                self.grantDb(dataName, d["app_id"], 'ga_data', ga_admin)
            if reporterName not in existsDb:
                self.grantDb(reporterName, d["app_id"], 'ga_reporter', ga_admin)
            if tagName not in existsDb:
                self.grantDb(tagName, d["app_id"], 'ga_tag', ga_admin)
            if configName not in existsDb:
                self.grantDb(configName, d["app_id"], 'ga_config', ga_admin)

    def grantDb(self, dName, app_id, dType, ga_admin):
        # 创建数据库
        self.conn_admin(ga_admin).execute("create database %s CHARACTER SET utf8" % dName)

        # 授权主机端口
        db_host = ga_admin["host"]
        db_port = ga_admin["port"]
        # 授权的数据库账号密码
        db_user = "%s_%s" % (dType, app_id) if dType != 'ga_reporter' else 'gr_%s' % app_id
        db_pwd = randStr(20)

        # 分配权限
        self.conn_admin(ga_admin).execute("GRANT all PRIVILEGES on %s.* to %s%s IDENTIFIED by '%s' with grant option" % (dName, db_user, "@'192.168.1.%'", db_pwd))

        # 更新ga_game配置
        db().execute("update ga_game set game_dbhost='%s',game_dbport='%s',game_dbname='%s',game_dbroot='%s',game_dbpwd='%s' where app_id='%s'" % (db_host, db_port, dName, db_user, db_pwd, app_id))

        # 插入ga_db配置
        db().execute("delete from ga_db where app_id='%s' and db='%s'" % (app_id, dType))
        db().execute("insert into ga_db(app_id, host, port, user, password, db) values('%s','%s','%s','%s','%s','%s') " % (app_id, db_host, db_port, db_user, db_pwd, dType))
        
    @staticmethod    
    def conn_admin(ga_admin):
        conn = db().conn(ga_admin['host'], ga_admin['user'], ga_admin['password'], "", ga_admin['port'])
        return conn