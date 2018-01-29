# -*- coding: utf-8 -*-
# Author:chensj
# 活跃分析
from active import *
class activeReportClass(activeJob):
    def beforeExecute(self):
        self.breakExecute = True

     # 自定义任务列表
    def taskDataList(self):
        return self.assignTask(byserver=False)

    def execute(self, myTaskDataList=[]):
        # for t in myTask:
        app_id = '20001'
        cur_time = self.curTime() - 24*3600
        self.activeModel(app_id, cur_time)

    def activeModel(self, app_id, cur_time):
        v_day = int(time.strftime('%Y%m%d', time.localtime(cur_time)))
        last_week_day = int(time.strftime('%Y%m%d', time.localtime(cur_time - 7 * 24 * 3600)))
        last_month_day = int(time.strftime('%Y%m%d', time.localtime(cur_time - 30 * 24 * 3600)))
        login_dict = self.vMap(app_id=app_id, table='d_login', where="v_day=" % v_day, counts={'u':'uid', 'p':'pid'})
        login_week_dict = self.vMap(app_id=app_id, table='d_login', where="v_day between %s and %s" % (last_week_day, v_day), counts={'u':'uid', 'p':'pid'})
        login_month_dict = self.vMap(app_id=app_id, table='d_login', where="v_day between %s and %s" % (last_month_day, v_day), counts={'u':'uid', 'p':'pid'})
        pay_dict = self.vMp(app_id=app_id, table='d_pay', where="v_day=" % v_day, counts={'u':'uid', 'p':'pid'})
        pay_week_dict = self.vMap(app_id=app_id, table='d_pay', where="v_day between %s and %s" % (last_week_day, v_day), counts={'u':'uid', 'p':'pid'})
        pay_month_dict = self.vMap(app_id=app_id, table='d_pay', where="v_day between %s and %s" % (last_month_day, v_day), counts={'u':'uid', 'p':'pid'})
        new_player_dict = self.vMap(app_id=app_id, table='d_player', where="v_day=%s" % v_day)
        new_player_week_dict = self.vMap(app_id=app_id, table='d_player', where="v_day between %s and %s" % (last_week_day, v_day))
        new_player_month_dict = self.vMap(app_id=app_id, table='d_player', where="v_day between %s and %s" % (last_month_day, v_day))
        new_user_dict = self.vMap(app_id=app_id, table='d_user', where="v_day=%s" % v_day)
        new_user_week_dict = self.vMap(app_id=app_id, table='d_user', where="v_day between %s and %s" % (last_week_day, v_day))
        new_user_month_dict = self.vMap(app_id=app_id, table='d_user', where="v_day between %s and %s" % (last_month_day, v_day))

        self.reduceAndInsert(app_id, v_day, login_dict, login_week_dict, login_month_dict, pay_dict, pay_week_dict, pay_month_dict, new_player_dict, new_player_week_dict, new_player_month_dict,
                             new_user_dict, new_user_week_dict, new_user_month_dict)


    def vMap(self, app_id, table='', where='', sumf='', counts={}, v_year=0):
        table = "%s:%s" % (table, v_year) if v_year else table
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
    def reduceAndInsert(app_id, v_day, login_dict, login_week_dict, login_month_dict, pay_dict, pay_week_dict, pay_month_dict, new_player_dict, new_player_week_dict, new_player_month_dict,
                        new_user_dict, new_user_week_dict, new_user_month_dict):
        table = 'r_active'
        sc_keys = list(
            set(login_dict.keys())
            .union(
                set(login_week_dict.keys()),
                set(login_month_dict.keys()),
                set(pay_dict.keys()),
                set(pay_week_dict.keys()),
                set(pay_month_dict.keys()),
                set(new_player_dict.keys()),
                set(new_player_week_dict.keys()),
                set(new_player_month_dict.keys()),
                set(new_user_dict.keys()),
                set(new_user_week_dict.keys()),
                set(new_user_month_dict.keys())
            )
        )

        if not sc_keys:
            return

        ddata = []
        for sc in sc_keys:
            data = dict()
            data['v_day'] = v_day
            data['sid'], data['channel_id'] = sc.split(',')
            data['dap'], data['dau'] = login_dict[sc]['p'], login_dict[sc]['u'] if sc in login_dict else 0
            data['wap'], data['wau'] = login_week_dict[sc]['p'], login_week_dict[sc]['u'] if sc in login_week_dict else 0
            data['map'], data['mau'] = login_month_dict[sc]['p'], login_month_dict[sc]['u'] if sc in login_month_dict else 0
            data['pdap'], data['pdau'] = pay_dict[sc]['p'], pay_dict[sc]['u'] if sc in pay_dict else 0
            data['pwap'], data['pwau'] = pay_week_dict[sc]['p'], pay_week_dict[sc]['u'] if sc in pay_week_dict else 0
            data['pmap'], data['pmau'] = pay_month_dict[sc]['p'], pay_week_dict[sc]['u'] if sc in pay_month_dict else 0
            data['ndap'] = new_player_dict[sc]['p'] if sc in new_player_dict else 0
            data['nwap'] = new_player_week_dict[sc]['p'] if sc in new_player_week_dict else 0
            data['nmap'] = new_player_month_dict[sc]['p'] if sc in new_player_month_dict else 0
            data['ndau'] = new_user_dict[sc]['u'] if sc in new_user_dict else 0
            data['nwau'] = new_user_week_dict['u'] if sc in new_user_week_dict else 0
            data['nmau'] = new_user_month_dict['u'] if sc in new_user_month_dict else 0
            ddata.append(data)
        # 删除今日数据
        db('ga_reporter', app_id).execute("delete from %s where v_day=%s " % (table, v_day))
        # 批量插入当日所有数据 up_data
        for i in range(0, len(ddata), 500):
            db_save_reporter(table=table, data=ddata[i:i+500], app_id=app_id)
