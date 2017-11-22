# -*- coding: utf-8 -*-
# Filename: include.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-07-24
# Author:       mpdesign
# description:  核心包
# -----------------------------------

import sys
reload(sys)
from comm.common import *
from db.mysql import *
from db.redisdb import *
from mp import *
from pdaemon import *
from worker import *


def sysConnMysql():
    return singleton.getinstance('mysql', 'core.db.mysql').conn(
        DEFAULT_DB['host'],
        DEFAULT_DB['user'],
        DEFAULT_DB['password'],
        DEFAULT_DB['db'],
        DEFAULT_DB['port']
    )


def sysConnRdb():
    return singleton.getinstance('redisdb', 'core.db.redisdb').conn(
        DEFAULT_REDIS['host'],
        DEFAULT_REDIS['port'],
        DEFAULT_REDIS['db']
    )

