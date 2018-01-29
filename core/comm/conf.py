# -*- coding: utf-8 -*-
# Filename: conf.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-11-06
# Author:       mpdesign
# description:  配置信息
# -----------------------------------

import sys
import os
import time
from config.config import *
import MySQLdb
import MySQLdb.cursors


def queryMysql(sqltext=''):
    conn = MySQLdb.Connect(
        DEFAULT_DB['host'],
        DEFAULT_DB['user'],
        DEFAULT_DB['password'],
        DEFAULT_DB['db'],
        int(DEFAULT_DB['port']),
        charset="utf8",
        connect_timeout=300
    )
    cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    line = cursor.execute(sqltext)
    res = cursor.fetchall()
    conn.commit()
    cursor.close()
    return res

if SLAVE_NODE not in locals().keys() or not SLAVE_NODE:
    slaves = queryMysql("select * from %s where db='slave'" % CONFIG_TABLE)
    if not isinstance(slaves, type((0, 1))) and isinstance(slaves[0], type({})):
        print "[%s @slave]" % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), 'slave node is not configured in mysql'
        sys.exit(0)
    SLAVE_NODE = []
    for slave in slaves:
        SLAVE_NODE.append({"ip": slave['host'], 'port': slave['port'], 'user': slave['user'], 'password': slave['password']})
