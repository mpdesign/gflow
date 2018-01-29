# -*- coding: utf-8 -*-
# Filename: baseModel.py

# -----------------------------------
# Revision:         2.0
# Date:             2018-01-18
# Author:           mpdesign
# -----------------------------------
from work.__work__ import *

class baseModel(workInterface):

    def __init__(self, table_name='', redis_db='base_lua', app_id=''):
        self.table_name = table_name
        self.redis_db = redis_db
        self.app_id = app_id


    def getTable(self, v_day):
        r_table = memory(redisConfig(redis_type=self.redis_db, app_id=self.app_id)).redisInstance().hgetall('%s%s_%s_%s' % (DB_PREFIX, self.table_name, self.app_id, v_day))
        return r_table

    def getLength(self, v_day):
        length = memory(redisConfig(redis_type=self.redis_db, app_id=self.app_id)).redisInstance().hlen('%s%s_%s_%s' % (DB_PREFIX, self.table_name, self.app_id, v_day))
        return length

