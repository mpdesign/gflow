# -*- coding: utf-8 -*-
# Author:chensj
# 新增用户分析

from test import *

class newBehaviorTask(testJob):
    def beforeExecute(self):
        self.breakExecute = True

     # 自定义任务列表
    def taskDataList(self):
        return self.assignTask(byserver=False)

    def execute(self, myTaskDataList=[]):
        # for t in myTask:
        app_id = '20001'
        cur_time = self.curTime()

    def behaviorModel(self, app_id, v_day):
        sql = "select pid, uid, ch_player, ch_user, sid, play_times, play_time, create_player_day, create_user_day from d_player_behavior"
        r_behavior = db('data', app_id).query(sql, 'all')
        player_dict = {}
        user_dict = {}
        if not emptyquery(r_behavior):
            for r in r_behavior:
                play_times = intval(r['play_times'])
                play_time = intval(r['play_time'])
                # 按角色
                pid = r['pid']
                sid = r['sid_player']
                ch_player = r['ch_player']
                sc = "%s, %s" % (sid, ch_player)
                create_player_day = r['create_player_day']
                if create_player_day not in player_dict:
                    player_dict[create_player_day] = {}
                if sc not in player_dict[v_day]:
                    player_dict[create_player_day][sc] = {}
                player_dict[create_player_day][sc]['pid'] = pid
                if 'play_times' in player_dict[create_player_day][sc]:
                    player_dict[create_player_day][sc]['play_times'] += play_times
                else:
                    player_dict[create_player_day][sc]['play_times'] = play_times
                if 'play_time' in player_dict[create_player_day][sc]:
                    player_dict[create_player_day][sc]['play_time'] += play_time
                else:
                    player_dict[create_player_day][sc]['play_time'] = play_time

                # 按账号
                uid = r['uid']
                sid = r['sid_user']
                ch_user = r['ch_user']
                sc = "%s, %s" % (sid, ch_user)
                create_user_day = r['create_user_day']
                if v_day not in user_dict:
                    player_dict[create_player_day] = {}
                if sc not in player_dict[v_day]:
                    player_dict[create_player_day][sc] = {}
                player_dict[create_player_day][sc]['pid'] = pid

