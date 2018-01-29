# -*- coding: utf-8 -*-
# Filename: record.py

# -----------------------------------
# Revision:         1.0
# Date:             2017-12-06
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      record
# frequency:        timely
# -----------------------------------

from dataInterface import *
from dataConfig import *

class recordTask(dataInterface):

    #默认执行方法
    def execute(self, taskDataList=[]):

        self.mutiWorker(myTask=taskDataList, schemes=schemes['record'], popKeyPre='record')
