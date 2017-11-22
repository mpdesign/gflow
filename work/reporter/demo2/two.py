# -*- coding: utf-8 -*-

from demo2 import *

class twoTask(demo2Job):

    def beforeExecute(self):
        self.atExecute = 'd1'

     # 自定义任务列表
    def mapTask(self):
        return DEFAULT_NODE

    def execute(self, myTask=[]):

        # output('two id1 %s' % id(singleton.getinstance('mysql', 'core.db.mysql')))
        # output('two id2 %s' % id(singleton.getinstance('mysql', 'core.db.mysql')))
        r = random.randint(0, 1)
        output('two ' + str(r))
        # 测试数据库连接
        for i in range(0, 1):
            app_id = [10001, 10002, 10003, 10005, 10010][random.randint(0,4)]
            db('data', app_id=app_id).query("select count(1) from d_click")
        return r

