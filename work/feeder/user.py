# -*- coding: utf-8 -*-
# Filename: user.py

# -----------------------------------
# Revision:         3.0
# Date:             2015-08-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      pop user data from redis to mysql
# frequency:        timely
# -----------------------------------

from feederInterface import *


class user(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_user',
            'fields': ["v_time", "v_hour", "v_day", "channel_id", "did", "uid", "username", "ip", "type", "area", "gender", 'adid'],
            'update': {'set': ['username', 'area', 'gender'], 'where': ['uid']}
        }
        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='user')
