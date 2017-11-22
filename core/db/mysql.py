# -*- coding: utf-8 -*-
"""
Created on 2015-03-24
@author: mpdesign
"""
import MySQLdb
import MySQLdb.cursors
from DBUtils.PooledDB import PooledDB
from core.comm.common import *


# 最大连接数
maxconnections = 200
# 尝试连接次数
TRY_CONNECT_TIMES = 100

_binname = argv_cli['argvs'][2] if 'argvs' in argv_cli.keys() and len(argv_cli['argvs']) > 2 else ''


class mysql:

    def __init__(self):
        self._debug = False
        self._code = []
        self.sqltext = ''
        self.pool_key = ''
        self.conn_key = ''
        self.cursor_key = ''

    def conn(self, host='', user='', passwd='', defaultdb='', port=3306):
        _key = "%s_%s_%s_%s" % (host, user, port, defaultdb)
        self.pool_key = "pool_%s" % _key
        self.conn_key = "conn_%s" % _key
        self.cursor_key = "cursor_%s" % _key
        if not hasattr(mysql, self.pool_key):
            # 连接池实例
            mysql.poolInstance(self.pool_key, host, user, passwd, defaultdb, port)

        return self

    # 创建连接池，根据服务器配置
    @staticmethod
    def poolInstance(pool_key='', host='', user='', passwd='', defaultdb='', port=3306):
        setattr(mysql, pool_key,
                PooledDB(creator=MySQLdb,
                         mincached=1,
                         maxcached=20,
                         host=host,
                         port=port,
                         user=user,
                         passwd=passwd,
                         db=defaultdb,
                         use_unicode=False,
                         charset='utf8',
                         cursorclass=MySQLdb.cursors.DictCursor,
                         maxconnections=maxconnections
                )
        )

    # 获取连接实例
    def connInstance(self):

        i = 0
        while i < TRY_CONNECT_TIMES:
            try:
                setattr(self,
                        self.conn_key,
                        getattr(mysql, self.pool_key).connection()
                )
                break
            except Exception, e:
                output('mysql Exception ' + str(e), log_type='mysql')
            j = 60 if i >= 4 else i*random.randint(1, 5)
            # 3次连接不上则发送警告，但不终止，继续尝试连接
            if i == 10 or i == 50:
                _msg = "Can't connect mysql %s@%s %s times" % (self.conn_key[5:], _binname, i)
                output(_msg, log_type='mysql')
                notice_me(_msg)
            time.sleep(j)
            i += 1
        # 超过连接次数则终止程序并发送警告
        if i >= TRY_CONNECT_TIMES:
            _msg = "Can't connect mysql %s@%s %s times, exit" % (self.conn_key[5:], _binname, i)
            output(_msg, log_type='mysql')
            notice_me(_msg)
            sys.exit(0)

        return get_attr(self, self.conn_key)

    # sql语句操作
    def execute(self, sqltext, args=None, many=False):
        dbconn = self.connInstance()
        cur = dbconn.cursor()
        self.sqltext = sqltext
        try:
            if many is False:
                execute_res = cur.execute(sqltext, args)
            else:
                execute_res = cur.executemany(sqltext, args)
            dbconn.commit()
        except Exception, e:
            result = self._error(e, t='cursor.execute')
            return result

        try:
            if many is False:
                result = cur.fetchall()
            else:
                result = cur.nextset()
            sqltext = sqltext.strip(' ')
            if sqltext[0:6] == 'delete' or sqltext[0:6] == 'update':
                return execute_res
        except Exception, e:
            result = self._error(e, t='execute.Exception')

        return result

    # sql语句查询
    def query(self, sqltext, f='one', rows=0):
        dbconn = self.connInstance()
        cur = dbconn.cursor()
        self.sqltext = sqltext
        try:
            cur.execute(sqltext)
            dbconn.commit()
        except Exception, e:
            result = self._error(e, t='query.Exception')
            return result

        try:
            if f == 'one':
                result = cur.fetchone()
            elif rows > 0:
                result = cur.fetchmany(rows)
            else:
                result = cur.fetchall()
            cur.close()
        except Exception, e:
            result = self._error(e, t='query.fetch.Exception')

        return result

    # 格式化语句执行
    def save(self, table='', data=None, conditions=None):
        if not table or not data:
            output('Table or data is None', log_type='mysql')
            return None
        if isinstance(data, type([])) and len(data) > 0 and isinstance(data[0], type({})):
            # 批量插入
            sql, data_values = self._mult_insert_sql(table, data)
            return self.execute(sql, args=data_values, many=True)
        elif isinstance(data, type({})):
            # 单条插入或更新操作
            sql = self._set_sql(table, data, conditions)
            return self.execute(sql)
        return None

    # 格式化查询
    def find(self, table='', fields='*', conditions=None, limit='', order='', f='one', rows=0):
        if fields and isinstance(fields, type([])) and len(fields) > 0:
            fields = '`,`'.join(fields)
        where = ''
        if conditions and isinstance(conditions, type({})) and len(conditions) > 0:
            for k in conditions:
                where += " and %s='%s' " % (k, conditions[k])
            where = "where %s " % where[4:]
        if limit:
            limit = "limit %s" % limit

        if order:
            order = "order by %s" % order
        sqltext = "select %s from %s %s %s %s" % (fields, table, where, order, limit)
        return self.query(sqltext, f, rows)

    @staticmethod
    def _mult_insert_sql(table='', data=None):
        data_keys = data[0].keys()
        fields = "`%s`" % '`,`'.join(data_keys)
        fields_len = len(data_keys)
        fields_ps = ''
        if fields_len < 1:
            return None
        else:
            for i in range(fields_len):
                fields_ps = "%s,%s" % (fields_ps, '%s')
        fields_ps = fields_ps[1:]
        sql = 'insert into `%s`(%s) values(%s) ' % (table, fields, fields_ps)
        data_values = []
        for d in data:
            data_value = []
            for k in data_keys:
                if k not in d or not d[k]:
                    data_value.append('')
                else:
                    data_value.append(d[k])
            data_values.append(data_value)
        return sql, data_values

    @staticmethod
    def _set_sql(table='', data=None, conditions=None):
        if conditions:
            sets = ''
            where = ''
            for k in conditions:
                where += " and `%s`='%s' " % (k, conditions[k])
            where = where[4:]

            for k in data:
                if str(data[k]).find('`') >= 0:
                    sets += ",`%s`=%s" % (k, data[k])
                else:
                    sets += ",`%s`='%s'" % (k, data[k])
            sets = sets[1:]

            sql = "update `%s` set %s where %s " % (table, sets, where)
        else:
            data_values = ''
            data_keys = data.keys()
            fields = "`%s`" % '`,`'.join(data_keys)
            for k in data:
                data_values += ",'%s'" % data[k]
            data_values = data_values[1:]
            sql = 'insert into `%s`(%s) values(%s) ' % (table, fields, data_values)

        return sql

    # 调试模式
    def debug(self, mode=True, code=[]):
        self._debug = mode
        self._code = code
        return self

    def _error(self, es, t=''):
        if not t: t = "MYSQL-ERROR: "
        try:
            errormsg = str(es)
            try:
                errorcode = intval(es[0])
            except Exception, ec:
                errorcode = 1
        except Exception, e:
            errormsg = 'MYSQL未知错误类型'
            errorcode = 0
        if errorcode > 0:
            if self._debug:
                if self._code and errorcode in self._code or not self._code:
                    output(t + ' ' + errormsg + ' SQL: %s' % self.sqltext, log_type='mysql')
            elif self._code and errorcode not in self._code:
                output(t + ' ' + errormsg + ' SQL: %s' % self.sqltext, log_type='mysql')
            self.debug()
        else:
            output(t + errormsg, log_type='mysql')

        return t + errormsg