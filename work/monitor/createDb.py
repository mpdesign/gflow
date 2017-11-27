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
        admin = db().query("select * from %s where db='admin' limit 1" % DB_TABLE_NAME)
        if emptyquery(admin):
            output("admin db config is empty", task_name=self.__class__.__name__)
        # 创建数据库
        admin['user'] = singleton.getinstance('pcode').decode(admin['user']) if intval(admin['user']) > 10000 else admin['user']
        admin['password'] = singleton.getinstance('pcode').decode(admin['password']) if intval(admin['password']) > 10000 else admin['password']
        databases = self.conn_admin(admin).query("SELECT * FROM information_schema.SCHEMATA", "all")
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
            dataName = "%sdata_%s" % (PREFIX_NAME, d["app_id"])
            reporterName = "%sreporter_%s" % (PREFIX_NAME, d["app_id"])
            tagName = "%stag_%s" % (PREFIX_NAME, d["app_id"])
            configName = "%sconfig_%s" % (PREFIX_NAME, d["app_id"])
            if dataName not in existsDb:
                self.grantDb(dataName, d["app_id"], 'data', admin)
            if reporterName not in existsDb:
                self.grantDb(reporterName, d["app_id"], 'reporter', admin)
            if tagName not in existsDb:
                self.grantDb(tagName, d["app_id"], 'tag', admin)
            if configName not in existsDb:
                self.grantDb(configName, d["app_id"], 'config', admin)

    def grantDb(self, dName, app_id, dType, admin):

        # 创建数据库
        self.conn_admin(admin).execute("create database %s CHARACTER SET utf8" % dName)

        # 授权主机端口
        db_host = admin["host"]
        db_port = admin["port"]
        # 授权的数据库账号密码
        db_user = "%s_%s" % (dType, app_id) if dType != 'reporter' else 'xr_%s' % app_id
        db_pwd = randStr(20)

        dType = PREFIX_NAME + dType

        # 分配权限
        self.conn_admin(admin).execute("GRANT all PRIVILEGES on %s.* to %s%s IDENTIFIED by '%s' with grant option" % (dName, db_user, "@'192.168.1.%'", db_pwd))

        # 插入_db配置
        db().execute("delete from %s where app_id='%s' and db='%s'" % (DB_TABLE_NAME, app_id, dType))
        db().execute("insert into %s(app_id, host, port, user, password, db) values('%s','%s','%s','%s','%s','%s') " % (DB_TABLE_NAME, app_id, db_host, db_port, db_user, db_pwd, dType))
        
    @staticmethod    
    def conn_admin(admin):
        conn = db().conn(admin['host'], admin['user'], admin['password'], "", admin['port'])
        return conn