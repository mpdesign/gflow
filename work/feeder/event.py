# -*- coding: utf-8 -*-
# Filename: event.py

# -----------------------------------
# Revision:         3.0
# Date:             2015-08-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      event
# frequency:        timely
# -----------------------------------

from feederInterface import *

class event(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_event',
            'fields': ["v_time", "v_day", "channel_id", "pid", "did", "sid", "eventID", "value"],
            'check': True
        }
        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='event')
