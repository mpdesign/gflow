# -*- coding: utf-8 -*-
# Filename: reporter.py

# -----------------------------------
# Revision:         2.0
# Date:             2017-08-15
# Author:           mpdesign
# description:      报表数据层
# frequency:        timely
# -----------------------------------

from work.__work__ import *


class reporterLayer(workInterface):
    def __init__(self):
        workInterface.__init__(self)
        self.registerJob = [
            'demoJob'
        ]