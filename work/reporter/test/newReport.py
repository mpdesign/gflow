# -*- coding: utf-8 -*-
# Author:chensj

from test import *

class newReportTask(testJob):
    def beforeExecute(self):
        self.breakExecute = True

     # 自定义任务列表
    def taskDataList(self):
        return self.assignTask(byserver=False)

    def execute(self, myTaskDataList=[]):
        # for t in myTask:
        app_id = '20001'
        cur_time = self.curTime()

    def newModel(self, app_id, cur_time):
        v_day = int(time.strftime('%Y%m%d', time.localtime(cur_time)))
        v_year = int(time.strftime('%Y', time.localtime(cur_time)))
         # 设备字典
        device_dict = self.v_map(app_id=app_id, table='d_device', where='v_day=' % v_day, counts={'d':'did'}, v_year=v_year)
        # 账户字典
        user_dict = self.v_map(app_id=app_id, table='d_user', where='v_day=' % v_day, counts={'u':'uid'}, v_year=v_year)
        # 角色字典
        player_dict = self.v_map(app_id=app_id, table='d_player', where='v_day=' % v_day, counts={'p':'pid'}, v_year=v_year)

        self.reduce_and_insert(app_id, v_day, player_dict, user_dict, device_dict)

    def v_map(self, app_id, table='', where='', sumf='', counts={}, v_year=0):
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
    def reduce_and_insert(app_id, v_day, player_dict, user_dict, device_dict):
        table = 'r_newly'
        sc_keys = list(
            set(player_dict.keys())
            .union(
                set(user_dict.keys()),
                set(device_dict.keys())
            )
        )

        if not sc_keys:
            return

        ddata = []
        for sc in sc_keys:
            data = dict()
            data['v_day'] = v_day
            data['sid'], data['channel_id'] = sc.split(',')
            data['player'] = player_dict[sc]['p'] if sc in player_dict else 0
            data['user'] = user_dict[sc]['u'] if sc in user_dict else 0
            data['device'] = device_dict[sc]['d'] if sc in device_dict else 0
            ddata.append(data)
        # 删除今日数据
        db('ga_reporter', app_id).execute("delete from %s where v_day=%s " % (table, v_day))
        # 批量插入当日所有数据 up_data
        for i in range(0, len(ddata), 500):
            db_save_reporter(table=table, data=ddata[i:i+500], app_id=app_id)