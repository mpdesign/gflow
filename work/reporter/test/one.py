# -*- coding: utf-8 -*-
# Author:chensj

from test import *

class oneTask(testJob):

    def beforeExecute(self):
        self.breakExecute = True

     # 自定义任务列表
    def taskDataList(self):
        return self.assignTask(byserver=False)

    def execute(self, myTaskDataList=[]):
        for t in myTaskDataList:
            app_id = t['app_id']
            year = time.strftime("%Y", time.localtime(self.curTime()))
            tname = "`%s:%s`" % ('d_login', year)
            sql = "select * from %s where v_day=20171205" % tname
            print self.queryLoop(sql)

        print 'complete'

    def queryLoop(self, sql='', limit=''):
        perpage = 2
        return map(self.queryPagination, [sql+' limit %s,%s' % (i*perpage, perpage) for i in range(0,10)])

    def queryPagination(self, sql):
        return  db('data', 20001).query(sql, 'all')
