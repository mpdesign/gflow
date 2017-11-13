# -*- coding: utf-8 -*-
# Filename: mission.py

# -----------------------------------
# Revision:         3.0
# Date:             2015-08-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      pop mission data from redis to mysql
# frequency:        timely
# -----------------------------------

from feederInterface import *

class mission(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_mission',
            'fields': ["v_time", "v_day", "channel_id", "pid", "level", "sid", "missionID", "status", "level_1", "level_2", "level_3"],
            'check': True
        }
        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='mission')
