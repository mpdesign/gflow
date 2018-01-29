# -*- coding: utf-8 -*-
# Filename: preReport.py

# -----------------------------------
# Revision:         2.0
# Date:             2018-01-23
# Author:           mpdesign
# -----------------------------------

from work.streaming.streaming import *


class preReportJob(workInterface):
    def __init__(self):
        workInterface.__init__(self)
        # 注册子任务列表
        self.registerTask = [
            'test',
            'playerBehavior'
        ]