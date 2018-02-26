# -*- coding: utf-8 -*-
# Filename: tail.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-07-26
# Author:       mpdesign
# description:  tail作业
# -----------------------------------

from work.collector.collector import *


class tailJob(collectorLayer):
    def __init__(self):
        collectorLayer.__init__(self)
        # 任务名注册表
        self.registerTask = [
            'dc'
        ]
