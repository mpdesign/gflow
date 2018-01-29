# -*- coding: utf-8 -*-
# Filename: mapper.py

# -----------------------------------
# Revision:         2.0
# Date:             2018-01-12
# Author:           mpdesign
# -----------------------------------


from demo import *


class mapperTask(demoJob):

    # return result
    def mapper(self, x=None):
        x = x.split(' ')[0]
        if not x:
            return None
        return '%s %s' % (x, 1)
        # return line record, callback for execute(), append to resultSet = [line, line, ...]