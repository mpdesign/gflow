# -*- coding: utf-8 -*-
# Filename: cfunc.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-11-06
# Author:       mpdesign
# description:  公共脚本
# -----------------------------------

import time
import sys
import os
import datetime
#import cPickle
import hashlib
import random
import socket
import string
from csort import *



def md5(_str):
    return hashlib.md5(_str).hexdigest()


# sql注入安全
def sql_escape(sql_str):
    sql_str = encode(sql_str)
    sql_str = str(sql_str)
    sql_str.replace("'", "")
    sql_str.replace('"', '')
    sql_str.replace('/*', '')
    return sql_str


# 编码
def encode(content='', tocode='utf-8', fromcode='utf-8'):
    if isinstance(content, unicode):
        content = content.encode(tocode)
    elif tocode != fromcode:
        content = content.decode(fromcode).encode(tocode)
    return content


# 字典列表
def itemDict(data, key):
    if not data or key not in data.keys():
        return ''
    else:
        return data[key]


# 字典列表
def itemList(data, key):
    if not data or key not in data:
        return ''
    else:
        return data[key]


# 判断变量是否存在
def isset(var):
    if var not in locals().keys():
        return False
    else:
        return True


# 判断变量是否为空
def isempty(var):
    if var is None or var is False or len(var) < 1:
        return True
    else:
        return False


# 强制取整
def intval(s):
    if not s:
        return 0
    try:
        i = int(s)
        return i
    except Exception:
        i = ''
        for v in s:
            if 48 <= ord(v) <= 57:
                i = '%s%s' % (i, v)
            else:
                break
        if i:
            i = int(i)
        else:
            i = 0
        return i


# 强制转化浮点型
def floatval(s):
    try:
        f = int(s)
        return f
    except ValueError:
        f = ''
        for v in s:
            if 48 <= ord(v) <= 57 or ord(v) == 46:
                f = '%s%s' % (f, v)
            else:
                break
        if f:
            if f[0:1] == '.':
                f = "0%s" % f
            f = float(f)
        else:
            f = 0.0
        return f


# 随机字符串
def randStr(length=8, chars=string.ascii_letters+string.digits):
    return ''.join([random.choice(chars) for i in range(length)])


# 数据库查询结果是否为空
def emptyquery(result):
    if not result:
        return True

    if isinstance(result, type({})):
        return False
    elif isinstance(result, type((0, 1))) and isinstance(result[0], type({})):
        return False

    return True


def get_attr(obj, k):
    if hasattr(obj, k):
        return getattr(obj, k)
    else:
        return None


def del_attr(obj, k):
    if hasattr(obj, k):
        return delattr(obj, k)
    else:
        return None