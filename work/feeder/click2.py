# -*- coding: utf-8 -*-
# Filename: click2.py

# -----------------------------------
# Revision:         3.0
# Date:             2016-12-22
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      click2
# frequency:        timely
# -----------------------------------

from feederInterface import *


class click2(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_click2',
            'fields': ['wid', 'adid', 'ip', 'channel_id', 'user_agent', 'unique_code',  'v_day', 'v_time'],
            'check': True
        }

        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='click2')
