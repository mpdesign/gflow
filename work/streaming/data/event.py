# -*- coding: utf-8 -*-
# Filename: event.py

# -----------------------------------
# Revision:         1.0
# Date:             2017-12-06
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      event
# frequency:        timely
# -----------------------------------

from dataInterface import *
from dataConfig import *

class eventTask(dataInterface):

    #默认执行方法
    def execute(self, taskDataList=[]):

        self.mutiWorker(myTask=taskDataList, schemes=schemes['event'], popKeyPre='event')

