# -*- coding: utf-8 -*-
# Filename: click.py

# -----------------------------------
# Revision:         3.0
# Date:             2015-08-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      click
# frequency:        timely
# -----------------------------------

from feederInterface import *


class click(feederInterface):

    #默认执行方法
    def execute(self, myTask=[]):
        schemes = {
            'table': 'd_click',
            'fields': ['v_hour', 'v_day', 'v_time', 'mac', 'idfa', 'ip', 'channel_id', 'sid', 'adext', 'adid'],
            'check': True
        }

        self.mutiWorker(myTask=myTask, schemes=schemes, popKeyPre='click')
