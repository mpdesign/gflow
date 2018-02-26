# -*- coding: utf-8 -*-

# 导入核心包
from common.common import *


class workInterface(mp):

    def __init__(self):
        self.app_id = ''
        self.dbName = ''
        self.tableName = ''
        mp.__init__(self)
        pass

