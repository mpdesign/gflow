# -*- coding: utf-8 -*-
# Filename: singleton.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-07-11
# Author:       mpdesign
# description:  单例模式
# -----------------------------------

import threading
import os
from conf import *


class singleton(object):

    # 定义静态变量实例
    __singleton = {}

    def __init__(self):
        pass

    @staticmethod
    def getinstance(cls='', package=''):
        # 线程唯一，保证线程安全
        cls_id = "%s_%s" % (cls, threading.currentThread().ident)
        if cls_id not in singleton.__singleton:
            if not package:
                package = "libs.%s" % cls
                if not os.path.isfile('%s/%s.py' % (PATH_CONFIG['project_path'], package.replace('.', '/'))):
                    package = "core.%s" % package
            exec("from %s import * " % package)
            cls_obj = eval(cls)
            singleton.__singleton[cls_id] = cls_obj()
        return singleton.__singleton[cls_id]
