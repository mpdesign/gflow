# -*- coding: utf-8 -*-
# Filename: pcode.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-07-25
# Author:       mpdesign
# description:  加密解密
# -----------------------------------

from common.common import *


# 拷贝项目
def executeBin(params={}):
    if 'en' not in params["dicts"].keys() and 'de' not in params["dicts"].keys():
        print 'Please input code str, -en or -de ', "\n"
        return
    if 'en' in params["dicts"].keys():
        print singleton.getinstance('pcode').encode(params["dicts"]['en'])
    else:
        print singleton.getinstance('pcode').decode(params["dicts"]['de'])


