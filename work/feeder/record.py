# -*- coding: utf-8 -*-
# Filename: record.py

# -----------------------------------
# Revision:         3.0
# Date:             2015-08-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      record
# frequency:        timely
# -----------------------------------

from feederInterface import *


class record(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_record',
            'fields': ["v_time", "v_day", "channel_id", "pid", "level", "sid", "vip", "missionID", "itemID", "itemNum", "currencyID", "currencyNum", "currencyRemain", "itemRemain"],
            'check': True,
            'tableMaxNum': 10000000
        }

        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='record')