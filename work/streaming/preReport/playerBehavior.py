# -*- coding: utf-8 -*-
# Author:chensj


from preReport import *
class preReportTask(preReportJob):
    def beforeExecute(self):
        self.breakExecute = True

     # 自定义任务列表
    def taskDataList(self):
        return self.assignTask(byserver=False)

    def execute(self, myTaskDataList=[]):
        # for t in myTask:
        app_id = '20001'
        cur_time = self.curTime()
        self.behaviorModel(app_id)

    def behaviorModel(self, app_id):
        table = 'd_player_behavior'
        y_day = int(time.strftime('%Y%m%d', time.localtime(self.curTime() - 24*3600)))
        start_time = int(time.mktime(time.strptime(y_day, '%Y%m%d')))
        end_time = start_time+86399
        # 从redis中获取每个角色每天的游戏时长、游戏次数
        offline_dict = memory(redis_config_name='base_lua').evallua('playTimes.lua', 3, start_time, end_time, app_id)
        # 从充值表中获取每天充值角色的充值日期、充值金额
        pay_dict = self.get_pay_dict(y_day, app_id)
        # 合并两个字典的key
        p_keys = list[set(offline_dict.keys().union(set(pay_dict.keys())))]
        # 拼接角色行为表
        behavior_dict = {}
        if not emptyquery(p_keys):
            for pid in p_keys:
                if pid not in behavior_dict:
                    behavior_dict[pid] = {}
                if pid in offline_dict:
                    behavior_dict[pid] = offline_dict[pid]
                if pid in pay_dict:
                    behavior_dict[pid] = dict(behavior_dict[pid], **behavior_dict[pid])

        # 更新角色行为表
        if behavior_dict:
            insert_values = []
            for pid, val in behavior_dict.items():
                data = val
                # 获取当前角色行为表该角色信息
                sql = "select play_times, play_time, play_days, first_pay_day from d_player_behavior where pid='%s'" % pid
                r_behavior = db('data', app_id).query(sql)
                # 若该表中不存在该角色
                if not r_behavior:
                    # 结合角色表的区服、渠道、创角时间
                    r_player = self.get_player_info(pid, app_id)
                    data['pid'] = pid
                    data['ch_player'] = r_player['channel_id']
                    data['sid'] = r_player['sid']
                    data['create_player_day'] = r_player['v_day']
                    # 结合账号表的渠道、创建账号时间，作为新记录插入表中
                    if 'uid' in val:
                        uid = val['uid']
                        r_user = self.get_user_info(uid, app_id)
                        data['ch_user'] = r_user['channel_id']
                        data['create_user_day'] = r_user['v_day']
                    insert_values.append(data)
                else:
                    # 若存在，结合行为表中历史记录，运算后更新该角色对应记录
                    play_times_history = intval(r_behavior['play_times'])
                    play_time_history = intval(r_behavior['play_time'])
                    play_days_history = intval(r_behavior['play_days'])
                    first_pay_day = r_behavior['first_pay_day']
                    data['play_times'] = data['play_times'] + play_times_history if 'play_times' in data else play_times_history
                    data['play_time'] = data['play_time'] + play_time_history if 'play_time' in data else play_time_history
                    data['play_days'] = play_days_history + 1
                    if first_pay_day > 0:
                        data['first_pay_day'] = first_pay_day
                    db_save_data(table=table, data=data, conditions={'pid':pid}, app_id=app_id)
                db_save_data(table=table, app_id=app_id, data=insert_values)



    # 获取付费字典
    def get_pay_dict(self, v_day, app_id):
        sql = "select pid, sum(payment) s from d_pay where v_day=%s group by pid" % v_day
        r_pay = db('data', app_id).query(sql, 'all')
        pay_dict = {}
        if not emptyquery(r_pay):
            for r in r_pay:
                pid = r['pid']
                if pid not in pay_dict:
                    pay_dict[pid] = {}
                pay_dict[pid]['first_pay_day'] = v_day
                pay_dict[pid]['payment'] = r['s']
        return pay_dict

    # 获取角色信息
    def get_player_info(self, pid, app_id):
        sql = "select pid, channel_id, sid, v_day from d_player where pid=%s" % pid
        r_player = db('data', app_id).query(sql)
        return r_player

    # 获取账户信息
    def get_user_info(self, uid, app_id):
        sql = "select uid, channel_id, v_day from d_user where uid=%s" % uid
        r_user = db('data', app_id).query(sql)
        return r_user