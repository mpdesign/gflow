# -*- coding: utf-8 -*-
# Filename: demo2Job.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-07-26
# Author:       mpdesign
# description:  作业控制器，一个作业包含多个任务
# -----------------------------------

from work.reporter.reporter import *


class demo2Job(reporterLayer):
    def __init__(self):
        reporterLayer.__init__(self)
        # 任务名注册表
        self.registerTask = [
            'two'
        ]
