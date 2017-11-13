# -*- coding: utf-8 -*-
# Filename: player.py

# -----------------------------------
# Revision:         3.0
# Date:             2015-08-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      pop player data from redis to mysql
# frequency:        timely
# -----------------------------------

from feederInterface import *


class player(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_player',
            'fields': ["v_time", "v_hour", "v_day", "channel_id", "did", "pid", "uid", "sid", "newdid", "pname", "level", "last_login_day", 'adid'],
            'update': {'set': ['level'], 'where': ['pid']}
        }
        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='player')