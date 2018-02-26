# -*- coding: utf-8 -*-
"""
Created on 2018-02-01
@author: mpdesign
"""

from pyhive import presto
from core.comm.common import *


# 最大连接数
maxconnections = 200
# 尝试连接次数
TRY_CONNECT_TIMES = 100

_binname = argv_cli['argvs'][2] if 'argvs' in argv_cli.keys() and len(argv_cli['argvs']) > 2 else ''


class prestodb:

    def __init__(self):
        self.conn_key = ''

    def conn(self, host, port='8080', catalog='hive', schema='default', username=None, password=None, poll_interval=1):

        _key = "%s_%s_%s_%s_%s" % (host, port, catalog, schema, username)
        self.conn_key = "conn_%s" % _key
        if not hasattr(prestodb, self.conn_key):
            # 连接池实例
            self.connInstance(host, port=port, catalog=catalog, schema=schema, username=username, password=password, poll_interval=poll_interval)

        return self

    # 获取连接实例
    def connInstance(self, host, port='8080', catalog='hive', schema='default', username=None, password=None, poll_interval=1):

        i = 0
        while i < TRY_CONNECT_TIMES:
            try:
                setattr(self,
                        self.conn_key,
                        presto.connect(
                            host,
                            port=port,
                            catalog=catalog,
                            schema=schema,
                            username=username,
                            password=password,
                            poll_interval=poll_interval
                        ).cursor()
                )
                break
            except Exception, e:
                output('prestodb Exception ' + str(e), logType='presto')
            j = 60 if i >= 4 else i*random.randint(1, 5)
            # 3次连接不上则发送警告，但不终止，继续尝试连接
            if i == 10 or i == 50:
                _msg = "Can't connect prestodb %s@%s %s times" % (self.conn_key[5:], _binname, i)
                output(_msg, logType='presto')
                notice_me(_msg)
            time.sleep(j)
            i += 1
        # 超过连接次数则终止程序并发送警告
        if i >= TRY_CONNECT_TIMES:
            _msg = "Can't connect presto %s@%s %s times, exit" % (self.conn_key[5:], _binname, i)
            output(_msg, logType='presto')
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
            output('prestodb.execute Exception ' + str(e), logType='presto')

        return result

    def cancel(self):
        get_attr(self, self.conn_key).cancel()

    def poll(self):
        return get_attr(self, self.conn_key).poll()