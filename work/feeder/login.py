# -*- coding: utf-8 -*-
# Filename: login.py

# -----------------------------------
# Revision:         3.0
# Date:             2015-08-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      pop login data from redis to mysql
# frequency:        timely
# -----------------------------------

from feederInterface import *

class login(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_login',
            'fields': ["v_time", "v_hour", "v_day", "channel_id", "pid", "did", "uid", "sid", "ip"]
        }
        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='login')

    @staticmethod
    def doWorker(popKey, schemes):
        # 取出数据
        rows_insert, rows_update = r2m_model.popData(popKey, schemes)
        # 批量插入
        if rows_insert:
            for app_id in rows_insert:
                data_insert = rows_insert[app_id]
                db_save_data(table=schemes['table'], data=data_insert, app_id=app_id)
                #更新玩家最后登入日
                data_reinsert = []
                for row in data_insert:
                    last_login_day = time.strftime('%Y%m%d', time.localtime(row['v_time']))
                    # sql = "update d_player set `last_login_day`=%s, `play_days`=`play_days` + 1 where pid='%s'" % (last_login_day, row['pid'])
                    # db('ga_data', app_id).execute(sql)
                    # 创建角色时同时登陆，可能因找不到角色而导致更新不成功
                    data = {"last_login_day": last_login_day, "play_days": "`play_days`+1"}
                    conditions = {"pid": row['pid']}
                    save_success = db_save_data(table="d_player", data=data, conditions=conditions, app_id=app_id)
                    if not save_success:
                        data_reinsert.append((data, conditions))
                # 重新更新一次
                if len(data_reinsert) > 0:
                    for data, conditions in data_reinsert:
                        db_save_data(table="d_player", data=data, conditions=conditions, app_id=app_id)