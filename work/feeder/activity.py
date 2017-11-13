# -*- coding: utf-8 -*-
# Filename: r2m.py

# -----------------------------------
# Revision:         3.0
# Date:             2015-08-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      pop activity data from redis to mysql
# frequency:        timely
# -----------------------------------

from feederInterface import *


class activity(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_device',
            'fields': ['v_hour', 'v_day', 'v_time', 'did', 'screen', 'osv', 'hd', 'gv', 'mac', 'idfa', 'ip', 'newdid', 'channel_id', 'sid', 'isbreak', 'ispirated', 'adid', 'wid']
        }

        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='activity')