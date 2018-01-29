# -*- coding: utf-8 -*-
# Filename: basePayModel.py

# -----------------------------------
# Revision:         2.0
# Date:             2018-01-19
# Author:           mpdesign
# -----------------------------------
from baseModel import *

class basePayModel(baseModel):

    def __init__(self, app_id):
        baseModel.__init__(self, table_name='base_pay', app_id=app_id)

    def getPayPlayers(self, v_day):
        return

    def getPayUsers(self, v_day):
        return
