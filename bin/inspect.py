# -*- coding: utf-8 -*-
# Filename: inspect.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-11-20
# Author:       mpdesign
# description:  任务检查器
# -----------------------------------

from common.common import *


# 拷贝项目
def executeBin(params={}):
    if 't' not in params['dicts']:
        print 'Pleas input -t taskFullName [layer.job.task]'
        return
    taskFullName = params['dicts']['t'].replace('.ins', '')

    ipnode = params['dicts']['n'] if 'n' in params['dicts'] else ''

    print inspect.read(taskFullName=taskFullName, ipnode=ipnode)


