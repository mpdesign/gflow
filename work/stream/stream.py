# -*- coding: utf-8 -*-
# Filename: stream.py

# -----------------------------------
# Revision:         2.0
# Date:             2017-08-15
# Author:           mpdesign
# description:      清洗队列数据并落地至data库
# frequency:        timely
# -----------------------------------

from work.__work__ import *


class streamJob(workInterface):
    def __init__(self):
        workInterface.__init__(self)
        # 注册子任务列表
        self.registerTask = [
            'activity',
            'login',
            'activityCallback',
            'activityCallbackFailed'
        ]