# -*- coding: utf-8 -*-
# Filename: active.py

# -----------------------------------
# Revision:     1.0
# Date:         2018-01-25
# Author:       mpdesign
# description:  作业控制器，一个作业包含多个任务
# -----------------------------------

from work.reporter.reporter import *

class activeJob(workInterface):
    def __init__(self):
        workInterface.__init__(self)
        # 任务名注册表
        self.registerTask = [
            'activeReport',
        ]

