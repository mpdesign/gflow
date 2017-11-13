# -*- coding: utf-8 -*-
# Filename: top.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-03-17
# Author:       mpdesign
# description:  查看所有slave的进程状态
# -----------------------------------

from common.common import *


def executeBin(params):
    jname = params["job"]
    singleton.getinstance('ptop').show(jname)