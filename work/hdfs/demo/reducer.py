# -*- coding: utf-8 -*-
# Filename: reducer.py

# -----------------------------------
# Revision:         2.0
# Date:             2018-01-12
# Author:           mpdesign
# -----------------------------------

from demo import *


class reducerTask(demoJob):

    def beforeReducer(self):
        pass

    # return result
    def reducer(self, x=None, y=None):
        if x:
            _x = x.split(' ')
            x1 = _x[0]
            x2 = intval(_x[1]) if len(_x) > 1 else 0
            if x1 in y:
                y[x1] += x2
            else:
                y[x1] = x2
        return y

    # return resultSet = [result, result ...]
    def afterReducer(self, resultSet=None):
        return resultSet

    # 此方法可选择实现，如：合并汇聚处理 resultSets = [(resultIndex, resultSet), (resultIndex, resultSet), ...]
    # return resultSets = idx, result
    def gather(self, resultSets=[]):
        return resultSets