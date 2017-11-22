# -*- coding: utf-8 -*-


import redis
from core.comm.common import *
import thread

TRY_CONNECT_TIMES = 100
# 连接池最大连接数
max_connections = 80
_binname = argv_cli['argvs'][2] if 'argvs' in argv_cli.keys() and len(argv_cli['argvs']) > 2 else ''


class redisdb:

    def __init__(self):
        self.pool_key = ''
        self.conn_key = ''
        self.cursor_key = ''

    # 连接
    def conn(self, host, port, db=0):
        _key = "%s_%s_%s" % (host, port, db)
        self.pool_key = "pool_%s" % _key
        self.conn_key = "conn_%s" % _key
        if not hasattr(redisdb, self.pool_key):
            # 单例连接池
            redisdb.poolInstance(self.pool_key, host, port, db)
        self.redisInstance()
        return self

    # 创建连接池，根据服务器配置
    @staticmethod
    def poolInstance(pool_key='', host='', port=0, db=0):
        setattr(redisdb, pool_key, redis.ConnectionPool(host=host, port=port, db=db, max_connections=max_connections))

    # 返回redis实例
    def redisInstance(self, reconn=False):
        if not hasattr(self, self.conn_key):
            reconn = True
        else:
            try:
                if get_attr(self, self.conn_key).ping():
                    reconn = False
                else:
                    reconn = True
            except Exception, e:
                output('redis connInstance.ping error ' + str(e), log_type='redis')
        if reconn:
            i = 0
            while i < TRY_CONNECT_TIMES:
                try:
                    setattr(self, self.conn_key, redis.StrictRedis(connection_pool=getattr(redisdb, self.pool_key)))
                    break
                except Exception, e:
                    output('redis Exception ' + str(e), log_type='redis')
                j = 60 if i >= 4 else i*random.randint(1, 5)
                # 3次连接不上则发送警告，但不终止，继续尝试连接
                if i == 10 or i == 50:
                    _msg = "Can't connect redis %s@%s %s times" % (self.conn_key[5:], _binname, i)
                    output(_msg, log_type='redis')
                    notice_me(_msg)
                time.sleep(j)
                i += 1
            # 连接不上则终止程序并发送警告
            if i >= TRY_CONNECT_TIMES:
                _msg = "Can't connect redis %s@%s %s times" % (self.conn_key[5:], _binname, i)
                output(_msg, log_type='redis')
                notice_me(_msg)
                sys.exit(0)
        return get_attr(self, self.conn_key)

    def ping(self):
        try:
            pinging = get_attr(self, self.conn_key).ping()
        except Exception, e:
            pinging = False
        return pinging

    def close(self, conn_key=''):
        if not conn_key:
            conn_key = self.conn_key
        try:
            get_attr(self, conn_key).execute_command('quit')
        except Exception, e:
            output('redis close ' + conn_key + str(e), log_type='redis')
        finally:
            del_attr(self, conn_key)
            output('redis close ' + conn_key, log_type='redis')

    def set(self, key, value, sec=False, j=False):
        if j is not False:
            value = singleton.getinstance('pjson').dumps(value)
        self.redisInstance().set(key, value)
        if sec is not False:
            self.redisInstance().expire(key, sec)

    def get(self, key, j=False):
        result = self.redisInstance().get(key)
        if j is not False:
            result = singleton.getinstance('pjson').loads(result)
        return result


    # 载入lua脚本 numkeys 可用的redis key个数
    def evallua(self, luafile, numkeys, *keys_and_args, **keyargs):

        # 删除所有脚本缓存
        if luafile == 'flush':
            self.redisInstance().script_flush()
            return

        # 载入config文件
        with open("%s/lua/config.lua" % PATH_CONFIG['project_path'], 'r') as f:
            luaconfigscript = f.read()
            try:
                self.redisInstance().evalsha(luaconfigscript, numkeys, *keys_and_args)
            except Exception as e:
                self.redisInstance().eval(luaconfigscript, numkeys, *keys_and_args)

        if luafile[-4:] == '.lua':
            luafilepath = luafile if luafile[0:1] == '/' else "%s/lua/%s" % (PATH_CONFIG['project_path'], luafile)
            with open(luafilepath, 'r') as f:
                luascript = f.read()
        else:
            luascript = luafile
        luascript_hash = hashlib.sha1(luascript).hexdigest()
        try:
            r = self.redisInstance().evalsha(luascript_hash, numkeys, *keys_and_args)
        except Exception as e:
            r = self.redisInstance().eval(luascript, numkeys, *keys_and_args)

        return r

    # def __del__(self):
    #     for conn_key in self.conn_keys:
    #         self.close(conn_key)