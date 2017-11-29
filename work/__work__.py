# -*- coding: utf-8 -*-

# 导入核心包
from common.common import *


class workInterface(mp):

    def __init__(self):
        self.app_id = ''
        self.dbname = ''
        self.tablename = ''
        mp.__init__(self)
        pass

    @staticmethod
    def game(app_id=''):
        game_key = "%s_%s" % (GAME_TABLE_NAME, app_id)
        g = memory(redisConfig(redis_type='cache', app_id=app_id)).get(game_key, j=True)
        if not g:
            sql = "select * from %s where app_id='%s' limit 1" % (GAME_TABLE_NAME, app_id)
            g = db().query(sql)
            memory(redisConfig(redis_type='cache', app_id=app_id)).set(game_key, g, 300, j=True)
        return g

    @staticmethod
    def games():
        games_key = GAME_TABLE_NAME + "s"
        games = memory(redis_config_name='cache').get(games_key, j=True)
        if not games:
            where_app_id = ''
            app_ids = APP_ID_RANGE.split(',')
            if len(app_ids) == 2:
                if intval(app_ids[0]) > 0:
                    where_app_id += " app_id >= '%s' and" % app_ids[0]
                if intval(app_ids[1]) > 0:
                    where_app_id += " app_id <= '%s' and" % app_ids[1]
            elif len(app_ids) == 1 and intval(app_ids[0]) > 0:
                where_app_id = " app_id = '%s' and" % app_ids[0]
            sql = "select * from %s where %s game_status=0" % (GAME_TABLE_NAME, where_app_id)
            games = db().query(sql, "all")
            if emptyquery(games):
                games = None
            memory(redis_config_name='cache').set(games_key, games, 300, j=True)
        return games

    @staticmethod
    def server(app_id='', sid=''):
        server_key = "c_server_%s_%s" % (app_id, sid)
        s = memory(redisConfig(redis_type='cache', app_id=app_id)).get(server_key, j=True)
        if not s:
            sql = "select * from c_server where app_id='%s' and server_id='%s' limit 1" % (app_id, sid)
            s = db().query(sql)
            memory(redisConfig(redis_type='cache', app_id=app_id)).set(server_key, s, 300, j=True)
        return s

    def servers(self, app_id=''):
        if not app_id:
            return []
        game_where = "app_id='%s' and" % app_id
        cur_time = self.curTime()
        sql = "select * from c_server where %s server_start<=%s and server_status=0" % (game_where, cur_time)
        servers = db().query(sql, "all")
        if emptyquery(servers):
            servers = None
        return servers

    @staticmethod
    def channels(app_id=''):
        if not app_id:
            return []
        channel_key = "c_channels_%s" % app_id
        channel = memory(redisConfig(redis_type='cache', app_id=app_id)).get(channel_key, j=True)
        if channel:
            return channel
        game_where = "app_id='%s' and" % app_id
        sql = "select * from c_channel where %s ch_status=0" % game_where
        channels = db().query(sql, "all")
        channel = []

        # 新版必须创建渠道ID

        if not emptyquery(channels):
            cid = []

            for c in channels:
                app_id = c['app_id']
                ac_id = "%s_%s" % (app_id, c['channel_id'])
                if ac_id not in cid:
                    cid.append(c['channel_id'])
                    ca = dict()
                    ca['id'] = c['id']
                    ca['channel_id'] = c['channel_id']
                    ca['ch_name'] = c['ch_name']
                    ca['app_id'] = c['app_id']
                    channel.append(ca)

        # 设置缓存和过期时间
        memory(redisConfig(redis_type='cache', app_id=app_id)).set(channel_key, channel, sec=30, j=True)
        return channel

    @staticmethod
    def channel(app_id='', channel_id=''):
        channel_key = "c_channel_%s_%s" % (app_id, channel_id)
        c = memory(redisConfig(redis_type='cache', app_id=app_id)).get(channel_key, j=True)
        if not c:
            sql = "select * from c_channel where app_id='%s' and channel_id='%s' limit 1" % (app_id, channel_id)
            c = db().query(sql)
            memory(redisConfig(redis_type='cache', app_id=app_id)).set(channel_key, c, 300, j=True)
        return c

    # 分配任务,按游戏、服务器、平台分配
    def assignTask(self, byserver=True):

        ts = []
        gs = []
        games = self.games()
        if not games:
            return []

        if not byserver:
            for g in games:
                gs.append({"app_id": g['app_id'], "assign_node": g['assign_node']})
            return gs
        for g in games:
            g_id = g['app_id']
            servers = self.servers(app_id=g_id)
            if not servers:
                continue
            for s in servers:
                t = dict()
                t['sid'] = intval(s['server_id'])
                t['app_id'] = g_id
                t['assign_node'] = g['assign_node']
                ts.append(t)

        return ts

    def assignTaskByChannel(self):

        tc = []
        games = self.games()
        if games:
            for g in games:
                channels = self.channels(app_id=g['app_id'])
                if channels:
                    for c in channels:
                        t = dict()
                        t['channel_id'] = c['channel_id']
                        t['app_id'] = c['app_id']
                        t['assign_node'] = g['assign_node']
                        tc.append(t)
        return tc

