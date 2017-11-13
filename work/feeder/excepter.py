# -*- coding: utf-8 -*-
# Filename: excepter.py

# -----------------------------------
# Revision:         3.0
# Date:             2015-08-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      excepter
# frequency:        timely
# -----------------------------------

from feederInterface import *


class excepter(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_exception',
            'fields': ["v_time", "v_day", "channel_id", "pid", "did", "e_name", "e_reason", "e_stack", "gv", "gv_build", "ip"]
        }
        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='excepter')
