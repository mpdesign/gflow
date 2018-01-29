# -*- coding: utf-8 -*-
# Filename: tail.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-07-26
# Author:       mpdesign
# description:  tail作业
# -----------------------------------

from work.reporter.reporter import *


class tailJob(reporterLayer):
    def __init__(self):
        reporterLayer.__init__(self)
        # 任务名注册表
        self.registerTask = [
            'dc'
        ]
