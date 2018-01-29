# -*- coding: utf-8 -*-
# Author:chensj

from test import *
import types

class testLuaTask(testJob):

    def beforeExecute(self):
        self.sleepExecute = 60

     # 自定义任务列表
    def taskDataList(self):
        return self.assignTask(byserver=False)

    def execute(self, myTaskDataList=[]):
        #测试用登录日志表数据
        data = {}
        data['pid'] = 1
        data['uid'] = 1
        data['did'] = 1
        data['v_time'] = self.curTime()
        data['channel_id'] = 1
        data['sid'] = 1
        json = singleton.getinstance('pjson').dumps(data).replace('\n', '')

        cur_day = int(time.strftime('%Y%m%d', time.localtime(self.curTime() - 24*3600)))

        memory(redis_config_name='xy_data').redisInstance().rpush('xy_login_log_20001', json)
        r = memory(redis_config_name='xy_data').evallua('test.lua', 2, 'xy_login_log_20001', cur_day, 20001)
        print r

        print 'test lua complete'