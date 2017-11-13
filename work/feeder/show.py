# -*- coding: utf-8 -*-
# Filename: show.py

# -----------------------------------
# Revision:         3.0
# Date:             2016-12-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      show
# frequency:        timely
# -----------------------------------

from feederInterface import *


class show(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_page',
            'fields': ['wid', 'adid', 'ip', 'channel_id', 'user_agent', 'unique_code',  'v_day', 'v_time'],
            'check': True
        }

        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='show')
