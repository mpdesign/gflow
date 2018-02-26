# -*- coding: utf-8 -*-
# Filename: collector.py

# -----------------------------------
# Revision:         2.0
# Date:             2017-08-15
# Author:           mpdesign
# description:      数据采集器
# -----------------------------------

from common.common import *


class collectorLayer(mp):
    def __init__(self):
        mp.__init__(self)
        self.registerJob = [
            'tail'
        ]