# -*- coding: utf-8 -*-
# Filename: cdate.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-09-05
# Author:       mpdesign
# description:  时间处理器
# -----------------------------------

import sys
reload(sys)
sys.setdefaultencoding('utf8')
import os
import time
from common import *


#日期转换为时间戳
def mktime(date=''):
    if not date:
        return 0
    dateArr = date.split(' ')
    ymd = dateArr[0].split('-')
    his = dateArr[1].split(':')
    if len(ymd) < 3 or len(his) < 3:
        return 0
    dateC = datetime.datetime(intval(ymd[0]), intval(ymd[1]), intval(ymd[2]), intval(his[0]), intval(his[1]), intval(his[2]))
    timestamp = time.mktime(dateC.timetuple())
    return timestamp


#一年中第几周转换时间戳 2014-01
def mktimew(date=''):
    if not date:
        return 0
    yw = date.split('-')
    yeartime = mktime("%s-01-01 00:00:00" % yw[0])
    #当年第一天第一周周几
    w0 = intval(time.strftime('%w', time.localtime(yeartime)))
    #第一周总的时间戳
    if w0 == 0:
        w0time = 24*3600
    else:
        w0time = (8 - w0) * 24*3600
    timestamp = yeartime + w0time + (intval(yw[1]) - 1) * 7*24*3600
    return timestamp


#m月份后的d日时间戳计算
def monthtime(m=0, d=0, curtime=0):
    if curtime < 1:
        curtime = time.time()
    _y = intval(time.strftime('%Y', time.localtime(curtime)))
    _m = intval(time.strftime('%m', time.localtime(curtime)))
    m = intval(m)
    if 0 < d < 32:
        _d = d
    else:
        _d = 1
    if _m + m > 12 or _m + m < 0:
        mm = intval(_m + m)
        #求余
        _m = mm % 12
        #求整
        y = intval(mm/12)
        _y += y
    elif _m + m == 0:
        _m = 12
        _y -= 1
    else:
        _m += m
    if _m < 10:
        _m = '0%s' % _m
    if _d < 10:
        _d = '0%s' % _d
    _date = '%s-%s-%s 00:00:00' % (_y, _m, _d)
    return mktime(_date)


# 定时执行时间，返回即将执行的剩余时间
def atTime(dateT='', timeTypes=['m', 'w', 'd', 'H', 'M']):
    timeType = dateT[0:1]
    if timeType not in timeTypes:
        return None
    kv = {}
    k = ''
    for s in dateT:
        if s == '0' or intval(s) > 0:
            if k:
                if k not in kv.keys():
                    kv[k] = ''
                kv[k] = '%s%s' % (kv[k], s)
        else:
            k = s
    r = {}
    for k in timeTypes:
        if k in kv.keys():
            r[k] = intval(kv[k])
        else:
            r[k] = 0

    if timeType == 'm':
        #月初到定点时间
        at_time = (r['m']-1) * 24 * 3600 + r['d'] * 3600 + r['H'] * 60 + r['M']
        #月初到当前时间
        cur_time = time.time() - monthtime(m=0)
        if cur_time < at_time:
            left_time = at_time - cur_time
        else:
            left_time = monthtime(m=1) + at_time - time.time()
    elif timeType == 'w':
        w = intval(time.strftime('%w', time.localtime()))
        if w < 1:
            w = 7
        if r['w'] < 1:
            r['w'] = 7
        #周一到定点时间
        at_time = (r['w'] - 1) * 24 * 3600 + r['d'] * 3600 + r['H'] * 60 + r['M']
        #周一到当前时间
        cur_time = time.time() - (mktime(time.strftime('%Y-%m-%d 00:00:00', time.localtime())) - (w-1)*24*3600)
        if cur_time < at_time:
            left_time = at_time - cur_time
        else:
            left_time = 7 * 24 * 3600 + at_time - cur_time
    elif timeType == 'd':
        #当天凌晨到定点时间
        at_time = r['d'] * 3600 + r['H'] * 60 + r['M']
        #当天凌晨到当前时间
        cur_time = time.time() - mktime(time.strftime('%Y-%m-%d 00:00:00', time.localtime()))
        if cur_time < at_time:
            left_time = at_time - cur_time
        else:
            left_time = 24 * 3600 + at_time - cur_time

    elif timeType == 'H':
        #当小时到定点时间
        at_time = r['H'] * 60 + r['M']
        #当小时到当前时间
        cur_time = time.time() - mktime(time.strftime('%Y-%m-%d %H:00:00', time.localtime()))
        if cur_time < at_time:
            left_time = at_time - cur_time
        else:
            left_time = 3600 + at_time - cur_time
    elif timeType == 'M':
        #当分到定点时间
        at_time = r['M']
        #当分到当前时间
        cur_time = time.time() - mktime(time.strftime('%Y-%m-%d %H:%M:00', time.localtime()))
        if cur_time < at_time:
            left_time = at_time - cur_time
        else:
            left_time = 60 + at_time - cur_time
    return left_time