# -*- coding: utf-8 -*-
# Filename: feeder.py

# -----------------------------------
# Revision:         2.0
# Date:             2017-08-15
# Author:           mpdesign
# description:      数据流中转器，默认先清洗队列数据并落地至data库，并分流发布至其他订阅端
# frequency:        timely
# -----------------------------------

from work.__work__ import *


class feederJob(workInterface):
    def __init__(self):
        workInterface.__init__(self)
        # 注册子任务列表
        self.registerTask = [
            'activity',
            'login',
            'user',
            'player',
            'event',
            'excepter',
            'record',
            'mission',
            'playerLevel',
            'click',
            'show',
            'click2',
            'activityCallback',
            'activityCallbackFailed'
        ]
        # 作业名
        self.jobName = 'feederJob'
        # 等待终止作业时间
        self.waiteForStopJobTime = 3