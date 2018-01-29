# -*- coding: utf-8 -*-
# Filename: user.py

# -----------------------------------
# Revision:         1.0
# Date:             2017-12-06
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      pop user data from redis to mysql
# frequency:        timely
# -----------------------------------

from dataInterface import *
from dataConfig import *

class userTask(dataInterface):

    #默认执行方法
    def execute(self, taskDataList=[]):

        self.mutiWorker(myTask=taskDataList, schemes=schemes['user'], popKeyPre='user')
