# -*- coding: utf-8 -*-
# Filename: activity.py

# -----------------------------------
# Revision:         1.0
# Date:             2017-12-06
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      pop activity data from redis to mysql
# frequency:        timely
# -----------------------------------

from dataInterface import *
from dataConfig import *

class activityTask(dataInterface):

    #默认执行方法
    def execute(self, myTaskDataList=[]):

        data = {}
        data['app_id'] = 20001
        data['did'] = 'd8d9c0ef8d43157a6ccd14c094874f75'
        data['ip'] = '218.85.126.226'
        data['v_time'] = time.time()
        memory(redisConfig(redis_type='data', app_id='10002')).redisInstance().rpush('xy_data_queue_activity_20001', singleton.getinstance('pjson').dumps(data))

        self.mutiWorker(myTask=myTaskDataList, schemes=schemes['activity'], popKeyPre='activity')

