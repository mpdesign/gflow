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

from dataInterface import *
from dataConfig import *

class loginTask(dataInterface):

    #默认执行方法
    def execute(self, taskDataList=[]):
        # 测试
        # data = {}
        # data['app_id'] = 20001
        # data['pid'] = 'test001'
        # data['ip'] = '218.85.126.226'
        # data['v_time'] = time.time()
        # memory(redisConfig(redis_type='data', app_id='10002')).redisInstance().rpush('xy_data_queue_login_20001', singleton.getinstance('pjson').dumps(data))

        self.mutiWorker(myTask=taskDataList, schemes=schemes['login'], popKeyPre='login')

    def doWorker(self, popKey, schemes):
        # 取出数据
        rows_insert, rows_update = dataModel.popData(popKey, schemes)
        # 批量插入
        if rows_insert:
            for app_id in rows_insert:
                data_insert = rows_insert[app_id]

                # 获取与地区组装后的插入数据
                data_insert = dataModel.combineAreaInsertFields(data_insert)
                db_save_data(table=schemes['table'], data=data_insert, app_id=app_id)

                #更新玩家最后登入日
                data_reinsert = []
                for row in data_insert:
                    last_login_day = time.strftime('%Y%m%d', time.localtime(row['v_time']))
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