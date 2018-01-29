# -*- coding: utf-8 -*-
# Filename: player.py

# -----------------------------------
# Revision:         1.0
# Date:             2017-12-06
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      pop player data from redis to mysql
# frequency:        timely
# -----------------------------------

from dataInterface import *
from dataConfig import *

class playerTask(dataInterface):

    #默认执行方法
    def execute(self, myTaskDataList=[]):
        data = {}
        data['app_id'] = 20001
        data['pid'] = 'test001'
        data['ip'] = '218.85.126.226'
        data['v_time'] = self.curTime()
        memory(redisConfig(redis_type='data', app_id='10002')).redisInstance().rpush('xy_data_queue_player_20001', singleton.getinstance('pjson').dumps(data))
        self.mutiWorker(myTask=myTaskDataList, schemes=schemes['player'], popKeyPre='player')
