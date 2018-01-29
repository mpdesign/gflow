# -*- coding: utf-8 -*-
# Filename: test.py

# -----------------------------------
# Revision:     1.0
# Date:         2017-12-06
# Author:       mpdesign
# description:  作业控制器，一个作业包含多个任务
# -----------------------------------

from work.reporter.reporter import *
from work.reporter.basePlayerModel import *

class testJob(workInterface):
    def __init__(self):
        workInterface.__init__(self)
        # 任务名注册表
        self.registerTask = [
            # 'one',
            # 'testLua',
            'dailyReport'
        ]
