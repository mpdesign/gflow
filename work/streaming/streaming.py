# -*- coding: utf-8 -*-
# Filename: streaming.py

# -----------------------------------
# Revision:         2.0
# Date:             2018-01-17
# Author:           mpdesign
# -----------------------------------

from work.__work__ import *


class streamingLayer(workInterface):
    def __init__(self):
        workInterface.__init__(self)
        self.registerJob = [
            'data'
        ]
