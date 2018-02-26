# -*- coding: utf-8 -*-
"""
Created on 2018-02-01
@author: mpdesign
"""

from pyhive import hive
from core.comm.common import *


# 最大连接数
maxconnections = 200
# 尝试连接次数
TRY_CONNECT_TIMES = 100

_binname = argv_cli['argvs'][2] if 'argvs' in argv_cli.keys() and len(argv_cli['argvs']) > 2 else ''


class phive:

    def __init__(self):
        self.conn_key = ''

    def conn(self, host=None, port=10000, username=None, schema='default', auth=None, password=None):

        _key = "%s_%s_%s_%s" % (host, port, username, schema)
        self.conn_key = "conn_%s" % _key
        if not hasattr(phive, self.conn_key):
            # 连接池实例
            self.connInstance(host=host, port=port, username=username, schema=schema, auth=auth, password=password)

        return self

    # 获取连接实例
    def connInstance(self, host=None, port=10000, username=None, schema='default', auth=None, password=None):

        i = 0
        while i < TRY_CONNECT_TIMES:
            try:
                setattr(self,
                        self.conn_key,
                        hive.connect(
                            host=host, port=port, username=username, database=schema, auth=auth, password=password
                        ).cursor()
                )
                break
            except Exception, e:
                output('hive Exception ' + str(e), logType='hive')
            j = 60 if i >= 4 else i*random.randint(1, 5)
            # 3次连接不上则发送警告，但不终止，继续尝试连接
            if i == 10 or i == 50:
                _msg = "Can't connect hive %s@%s %s times" % (self.conn_key[5:], _binname, i)
                output(_msg, logType='hive')
                notice_me(_msg)
            time.sleep(j)
            i += 1
        # 超过连接次数则终止程序并发送警告
        if i >= TRY_CONNECT_TIMES:
            _msg = "Can't connect hive %s@%s %s times, exit" % (self.conn_key[5:], _binname, i)
            output(_msg, logType='hive')
            notice_me(_msg)
            _exit(0)

        return get_attr(self, self.conn_key)

    # sql语句操作
    def query(self, sqltext, f='all', parameters=None):
        result = None
        try:
            get_attr(self, self.conn_key).execute(sqltext, parameters=parameters)
            if f == 'one':
                result = get_attr(self, self.conn_key).fetchone()
            else:
                result = get_attr(self, self.conn_key).fetchall()
        except Exception, e:
            result = str(e)
            output('hive.execute Exception ' + str(e), logType='hive')

        return result

    def cancel(self):
        get_attr(self, self.conn_key).cancel()

    def poll(self):
        return get_attr(self, self.conn_key).poll()