# -*- coding: utf-8 -*-

from tail import *


class dcTask(tailJob):

    def beforeExecute(self):
        self.breakExecute = 300

    def taskDataList(self):
        return [{'assign_node': curNode()}]

    def execute(self, myTaskDataList=[]):
        singleton.getinstance('tail').conf(sourceFolder='/data/logs/nginx/api_dc.access_log').follow()

    # def afterExecute(self):
    #     output(('one resultSet', self.resultSet))
    #     output(('one resultSets', self.resultSets))