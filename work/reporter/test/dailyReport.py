# -*- coding: utf-8 -*-
# Author:chensj

from test import *

class dailyReportTask(testJob):
    def beforeExecute(self):
        self.breakExecute = True

     # 自定义任务列表
    def taskDataList(self):
        return self.assignTask(byserver=False)

    def execute(self, myTaskDataList=[]):
        # for t in myTask:
        app_id = '20001'
        cur_time = self.curTime()
        self.dailyModel(app_id, cur_time)

    def dailyModel(self, app_id, cur_time):
        print 11
        v_day = int(time.strftime('%Y%m%d', time.localtime(cur_time)))
        v_year = int(time.strftime('%Y', time.localtime(cur_time)))
        print basePlayerModel(app_id).getPlayers(v_day)
        print 22
        exit(0)
        # 设备字典
        device_dict = self.vMap(app_id=app_id, table='d_device', where='v_day=' % v_day, counts={'d':'did'}, v_year=v_year)
        # 账户字典
        user_dict = self.vMap(app_id=app_id, table='d_user', where='v_day=' % v_day, counts={'u':'uid'}, v_year=v_year)
        # 角色字典
        player_dict = self.vMap(app_id=app_id, table='d_player', where='v_day=' % v_day, counts={'p':'pid'}, v_year=v_year)
        # 活跃字典
        active_dict = self.vMap(app_id=app_id, table='d_login', where='v_day=%s' % v_day, counts={'p':'pid', 'u':'uid', 'd':'did'}, v_year=v_year)
        # 付费字典
        pay_dict = self.vMap(app_id, table='d_pay', where='v_day=%s' % v_day, sumf='payment', counts={'p':'player_id', 'u':'user_id', 'd':'device_id'}, v_year=v_year)
        # 新增付费字典(按角色）
        first_pay_dict_p = self.vMap(app_id, table='first_pay_p', where='v_day=%s' % v_day, sumf='payment', counts={'p:pid'}, v_year=v_year)
        # 新增付费字典（按玩家）
        first_pay_dict_u = self.vMap(app_id, table='first_pay_u', where='v_day=%s' % v_day, sumf='payment', counts={'u:uid'}, v_year=v_year)
        # 新增付费字典（按设备）
        first_pay_dict_d = self.vMap(app_id, table='first_pay_d', where='v_day=%s' % v_day, sumf='payment', counts={'d:did'}, v_year=v_year)

        print active_dict
        self.reduceAndInsert(app_id, v_day, player_dict, user_dict, device_dict, active_dict, pay_dict, first_pay_dict_p, first_pay_dict_u, first_pay_dict_d)

    def vMap(self, app_id, table='', where='', sumf='', counts={}, v_year=0):
        table = "%s:%s" % (table, v_year)
        fields = 'sid, channel_id'
        fields = fields if not sumf else '%s,%s' % (fields, sumf)
        if counts:
            for k,v in counts.items():
                fields = '%s,%s' % (fields, v)
        data = {}
        step = 30000
        start = 0
        while True:
            limit = "%s,%s" % (start, step)
            if table == 'first_pay_p':
                player_table = "d_player:%s" % v_year
                pay_table = "d_pay:%s" % v_year
                sql = "select %s from (select pid, channel_id, sid from `%s` where v_day='%s') a inner join (select player_id, payment from `%s` where v_day='%s') b on a.pid=b.player_id limit %s" % (fields, player_table, where, pay_table, where, limit)
            elif table == 'first_pay_u':
                user_table = "d_user:%s" % v_year
                pay_table = "d_pay:%s" % v_year
                sql = "select %s from (select uid, channel_id, sid from `%s` where v_day='%s') a inner join (select user_id, payment from `%s` where v_day='%s') b on a.uid=b.user_id limit %s" % (fields, user_table, where, pay_table, where, limit)
            elif table == 'first_pay_d':
                device_table = "d_device:%s" % v_year
                pay_table = "d_pay:%s" % v_year
                sql = "select %s from (select uid, channel_id, sid from `%s` where v_day='%s') a inner join (select device_id, payment from `%s` where v_day='%s') b on a.did=b.device_id limit %s" % (fields, device_table, where, pay_table, where, limit)
            else:
                sql = "select %s from `%s` where %s limit %s" % (fields, table, where, limit)
            result = db('xy_data', app_id).query(sql, 'all')
            start += step
            if emptyquery(result):
                break

            for r in result:
                ac = "%s,%s" % (r['sid'], r['channel_id'])
                if ac not in data:
                    data[ac] = {}
                    data[ac]['c'] = 0
                    if sumf: data[ac]['s'] = 0
                    if counts:
                        for k,v in counts.items():
                            data[ac][v] = {}
                data[ac]['c'] += 1
                if sumf: data[ac]['s'] += r[sumf]
                if counts:
                    for k,v in counts.items():
                        data[ac][v][r[v]] = 1
        if counts and data:
            for k,v in counts.items():
                data[ac][k] = len(data[ac][v])
        return data
    # 合并插入数据
    def reduceAndInsert(app_id, v_day, player_dict, user_dict, device_dict, active_dict, pay_dict, first_pay_dict_p, first_pay_dict_u, first_pay_dict_d):
        table = 'r_summary_d'
        sc_keys = list(
            set(player_dict.keys())
            .union(
                set(user_dict.keys()),
                set(device_dict.keys()),
                set(active_dict.keys()),
                set(pay_dict.keys()),
                set(first_pay_dict_p.keys()),
                set(first_pay_dict_u.keys()),
                set(first_pay_dict_d.keys())
            )
        )

        if not sc_keys:
            return

        values = []
        for sc in sc_keys:
            data = dict()
            data['v_day'] = v_day
            data['sid'], data['channel_id'] = sc.split(',')
            data['player'] = player_dict[sc]['p'] if sc in player_dict else 0
            data['user'] = user_dict[sc]['u'] if sc in user_dict else 0
            data['device'] = device_dict[sc]['d'] if sc in device_dict else 0
            data['dap'], data['dau'], data['dad'] = active_dict[sc]['p'], active_dict[sc]['u'], active_dict[sc]['d'] if sc in active_dict else 0
            data['ppfd'], data['ppfd_amount'] = first_pay_dict_p[sc]['p'], first_pay_dict_p[sc]['s'] if sc in first_pay_dict_p else 0
            data['pufd'], data['pufd_amount'] = first_pay_dict_u[sc]['u'], first_pay_dict_u[sc]['s'] if sc in first_pay_dict_u else 0
            data['pdfd'], data['pdfd_amount'] = first_pay_dict_d[sc]['d'], first_pay_dict_d[sc]['s'] if sc in first_pay_dict_d else 0
            data['payment'], data['pplayer'], data['puser'], data['pdevice'] = pay_dict[sc]['s'], pay_dict[sc]['p'], pay_dict[sc]['u'], pay_dict[sc]['d'] if sc in pay_dict else 0
            values.append(data)
        # 删除今日数据
        db('ga_reporter', app_id).execute("delete from %s where v_day=%s " % (table, v_day))
        # 批量插入当日所有数据 up_data
        for i in range(0, len(values), 500):
            db_save_reporter(table=table, data=values[i:i+500], app_id=app_id)
