# -*- coding: utf-8 -*-

from demo import *


class oneTask(demoJob):

    def beforeExecute(self):
        self.sleepExecute = 300
        # self.waiteForTask = 'reporter.demo2.two'
        # self.ifDoResultSets = True

     # 自定义任务列表
    def mapTask(self):
        # 返回所有任务，用于指派给所有slave
        # 格式：list
        #return [1, 2, 6, 9, 7, 3]

        # 指定只允许某个slave运行该任务
        # return ip
        # 按游戏
        return self.assignTask()

    def execute(self, myTask=[]):
        # print 'myTask', myTask
        # output('one id1 %s' % id(singleton.getinstance('mysql', 'core.db.mysql')))
        # output('one id2 %s' % id(singleton.getinstance('mysql', 'core.db.mysql')))
        r = []
        for i in range(0, 10):
            r.append(random.randint(0, 1000000))
        time.sleep(random.randint(0, 10))
        # output('r ' + str(r))

        # 测试数据库连接
        for i in range(0, 1):
            db().query("select * from xy_channel", "all")
        for i in range(0, 1):
            app_id = [10001, 10002, 10003, 10005, 10010][random.randint(0,4)]
            db('data', app_id=app_id).query("select count(1) from d_click")
        return r

    # def afterExecute(self):
    #     output(('one resultSet', self.resultSet))
    #     output(('one resultSets', self.resultSets))