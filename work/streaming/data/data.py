# -*- coding: utf-8 -*-
# Filename: data.py

# -----------------------------------
# Revision:         2.0
# Date:             2018-1-15
# Author:           mpdesign
# -----------------------------------

from work.streaming.streaming import *


class dataJob(workInterface):
    def __init__(self):
        workInterface.__init__(self)
        # 注册子任务列表
        self.registerTask = [
            'activity',
            'login',
            'activityCallback',
            'activityCallbackFailed',
            'ip'
        ]