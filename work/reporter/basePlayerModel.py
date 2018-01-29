# -*- coding: utf-8 -*-
# Filename: basePlayerModel.py

# -----------------------------------
# Revision:         2.0
# Date:             2018-01-19
# Author:           mpdesign
# -----------------------------------
from baseModel import *

class basePlayerModel(baseModel):

    def __init__(self, app_id):
        baseModel.__init__(self, table_name='base_player', app_id=app_id)

    def getPlayers(self, v_day):
        return self.getTable(v_day)

    def getPlayerNum(self, v_day):
        return self.getLength(v_day)
