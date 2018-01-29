# -*- coding: utf-8 -*-
# Filename: collector.py

# -----------------------------------
# Revision:         2.0
# Date:             2017-08-15
# Author:           mpdesign
# description:      数据采集器
# -----------------------------------

from work.__work__ import *


class collectorLayer(workInterface):
    def __init__(self):
        workInterface.__init__(self)
        self.registerJob = [
            'tail'
        ]