# -*- coding: utf-8 -*-
# Filename: monitor.py

# -----------------------------------
# Revision:         2.0
# Date:             2017-11-06
# Author:           mpdesign
# description:      监控器：游戏库监听创建、数据预警通知
# frequency:        sleepExecute
# -----------------------------------

from work.__work__ import *


class monitorJob(workInterface):
    def __init__(self):
        workInterface.__init__(self)
        self.registerTask = [
            'createDb'
            'alterTable'
        ]