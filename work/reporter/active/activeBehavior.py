# -*- coding: utf-8 -*-
# Author:chensj
# 活跃-游戏行为分析
from active import *
class activeBehaviorClass(activeJob):
    def beforeExecute(self):
        self.breakExecute = True

     # 自定义任务列表
    def taskDataList(self):
        return self.assignTask(byserver=False)

    def execute(self, myTaskDataList=[]):
        # for t in myTask:
        app_id = '20001'
        cur_time = self.curTime()


    def behaviorModel(self, app_id, cur_time):
        table = 'r_active_behavior'
        y_day = int(time.strftime('%Y%m%d', time.localtime(cur_time - 24*3600)))
        start_time = int(time.mktime(time.strptime(y_day, '%Y%m%d')))
        end_time = start_time+86399
        # 从redis中获取每个角色每天的游戏时长、游戏次数
        offline_dict = memory(redis_config_name='base_lua').evallua('playTimes.lua', 3, start_time, end_time, app_id)
        behavior_dict = {}
        values = []
        if offline_dict:
            for pid, val in offline_dict.items():
                sid = val['sid']
                channel_id = val['channel_id']
                uid = val['uid']
                play_times = intval(val['player_times'])
                play_time = intval(val['play_time'])
                sc = "%s,%s" % (sid, channel_id)
                if sc not in behavior_dict:
                    behavior_dict[sc] = {}
                if 'player' not in behavior_dict[sc]:
                    behavior_dict[sc]['player'] = {}
                if 'user' not in behavior_dict[sc]:
                    behavior_dict[sc]['user'] = {}
                if 'play_times' not in behavior_dict[sc]:
                    behavior_dict[sc]['play_times'] = 0
                if 'player' not in behavior_dict[sc]:
                    behavior_dict[sc]['play_time'] = 0
                behavior_dict[sc]['player'][pid] = 1
                behavior_dict[sc]['user'][uid] = 1
                behavior_dict[sc]['play_times'] += play_times
                behavior_dict[sc]['play_time'] += play_time

            # 根据行为字典，
            if behavior_dict:
                for sc,val in behavior_dict.items():
                    data = dict()
                    data['v_day'] = y_day
                    data['sid'], data['channel_id'] = sc.split(',')
                    data['dap'] = len(val['player'])
                    data['dau'] = len(val['user'])
                    data['play_times'] = val['play_times']
                    data['play_time'] = val['play_time']
                    values.append(data)

            # 删除今日数据
            db('ga_reporter', app_id).execute("delete from %s where v_day=%s " % (table, y_day))
            # 批量插入当日所有数据 up_data
            for i in range(0, len(values), 500):
                db_save_reporter(table=table, data=values[i:i+500], app_id=app_id)




