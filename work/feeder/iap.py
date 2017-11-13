# -*- coding: utf-8 -*-
# Filename: iap.py

# -----------------------------------
# Revision:         3.0
# Date:             2017-01-03
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      iap
# frequency:        timely
# -----------------------------------

from feederInterface import *


class iap(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_iap',
            'fields': ['v_time', 'v_day', 'did', 'oid', 'sid', 'channel_id', 'pid', 'info', 'key', 'value', 'ip'],
            'check': True
        }

        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='iap')
