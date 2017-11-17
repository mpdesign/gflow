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
from config.config import *
import socket
import string



def md5(_str):
    return hashlib.md5(_str).hexdigest()


#sql注入安全
def sql_escape(sql_str):
    sql_str = encode(sql_str)
    sql_str = str(sql_str)
    sql_str.replace("'", "")
    sql_str.replace('"', '')
    sql_str.replace('/*', '')
    return sql_str


# ---------------------
#获取当前机器ip eth=3 取全部IP eth=0 取当前IP
# ---------------------
def ipaddress(eth=3):
    if DEBUG_MODE > 0:
        return [i['ip'] for i in SLAVE_NODE]
    try:
        ips = socket.gethostbyname(socket.gethostname()).strip()
        if ips[0:len(LAN_IP_PREFIX)] == LAN_IP_PREFIX:
            if eth == 3:
                ips = [ips, '127.0.0.1']
            return ips
    except Exception, e:
        pass
    ip_cmd = "LANG=C /sbin/ifconfig $NIC | awk '/inet addr:/{ print $2 }' | awk -F: '{print $2 }'"
    ips = os.popen(ip_cmd).read().strip()
    if not ips:
        ip_cmd = "LANG=C /sbin/ifconfig $NIC | awk '/inet /{ print $2 }'"
        ips = os.popen(ip_cmd).read().strip()
    ipl = ips.split('\n')
    if eth > 2 or eth < 0:
        return ipl
    else:
        for i in ipl:
            for j in SLAVE_NODE:
                if j['ip'] == i:
                    return i
        return ipl[eth]

# ---------------------
# 终端输入参数变量
# ---------------------
argvs_term = {}


#获取终端输入参数
def getargvs(argv_list=[]):
    if len(argv_list) < 1:
        global argvs_term
        if len(argvs_term) > 1:
            return argvs_term
        argv_list = sys.argv[0:]
    params = {}
    argvs = []
    dicts = {}
    i = 0

    for p in argv_list:
        argv_len = len(argv_list)
        if p[0:1] == '-':
            k = i + 1
            if argv_len > k:
                if p[1:]:
                    if argv_list[k][0:1] != '-':
                        dicts[p[1:]] = argv_list[k]
                        del argv_list[k]
                    else:
                        dicts[p[1:]] = ''
            else:
                dicts[p[1:]] = ''
                break
        else:
            argvs.append(p)
        i += 1

    params["argvs"] = argvs
    params["dicts"] = dicts
    argvs_term = params
    return params

argv_cli = getargvs()


#编码
def encode(content='', tocode='utf-8', fromcode='utf-8'):
    if isinstance(content, unicode):
        content = content.encode(tocode)
    elif tocode != fromcode:
        content = content.decode(fromcode).encode(tocode)
    return content


#字典列表
def itemDict(data, key):
    if not data or key not in data.keys():
        return ''
    else:
        return data[key]


#字典列表
def itemList(data, key):
    if not data or key not in data:
        return ''
    else:
        return data[key]


#判断变量是否存在
def isset(var):
    if var not in locals().keys():
        return False
    else:
        return True


#判断变量是否为空
def isempty(var):
    if var is None or var is False or len(var) < 1:
        return True
    else:
        return False


#强制取整
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


#强制转化浮点型
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


#快速排序算法
def quickSort(data, order='asc', by=''):
    if len(data) > 1000:
        sys.setrecursionlimit(3000)
    if len(data) > 3000:
        return
    return subQuickSort(data, 0, len(data)-1, order=order, by=by)


def subQuickSort(data, start_index, end_index, order='asc', by=''):
    i = start_index
    j = end_index
    isdict = True if isinstance(data[j], type({})) and by in data[j].keys() else False
    if i >= j:
        return data
    flag = data[i][by] if isdict else data[i]
    flagitem = data[i]
    while i < j:
        while i < j:
            compare_value = data[j][by] if isdict else data[j]
            rightfind = (compare_value < flag) if order == 'asc' else (compare_value > flag)
            if rightfind:
                break
            j -= 1
        data[i] = data[j]

        while i < j:
            compare_value = data[i][by] if isdict else data[i]
            leftfind = (compare_value > flag) if order == 'asc' else (compare_value < flag)
            if leftfind:
                break
            i += 1
        data[j] = data[i]

    data[i] = flagitem
    subQuickSort(data, start_index, i - 1, order=order, by=by)
    subQuickSort(data, j + 1, end_index, order=order, by=by)
    return data


# 判断当前是否master
def isMaster():
    iphost = ipaddress()
    if MASTER_NODE["ip"] in iphost:
        return MASTER_NODE["ip"]
    return False


def isSlave():
    iphost = ipaddress()
    for ip2 in SLAVE_NODE:
        if ip2['ip'] in iphost:
            return ip2['ip']
    return False


# 获取当前节点IP
def curNode():
    node_ip = isMaster() if isMaster() else isSlave()
    return node_ip


# 随机字符串
def randStr(length=8, chars=string.ascii_letters+string.digits):
    return ''.join([random.choice(chars) for i in range(length)])


# 动态分发任务数据
def distributeTaskData(data=None):
    iphost = ipaddress()
    if not data:
        return None
    if isinstance(data, type(1)):
        data = range(0, data)
    if not isinstance(data, type([])):
        print '[%s @%s] distribute data type must be list' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), argv_cli['argvs'][min(len(argv_cli['argvs']), 3) - 1])
        sys.exit(0)
    iplong = []

    # 如果数据包含app_id, 则按游戏划分
    isbyapp = False
    # 剔除有指定节点的任务
    assign_app = {}
    if isinstance(data[0], type({})) and 'app_id' in data[0].keys():
        all_data_by_app = {}
        un_assign_app = {}
        for t in data:
            if 'assign_node' in t.keys():
                if t['assign_node'] not in assign_app:
                    assign_app[t['assign_node']] = []
                assign_app[t['assign_node']].append(t['app_id'])
            else:
                un_assign_app[t['app_id']] = 1
            if t['app_id'] not in all_data_by_app:
                all_data_by_app[t['app_id']] = []
            all_data_by_app[t['app_id']].append(t)
        data = un_assign_app.keys()
        data.sort()
        isbyapp = True

    # 当前节点ip long
    cur_ip = ''
    cur_iplong = ''
    # 剩余可分配的节点
    for node_ip in SLAVE_NODE:
        il = reduce(lambda x, y: (x << 8) + y, map(int, node_ip["ip"].split('.')))
        # 剔除指定节点
        if node_ip["ip"] not in assign_app:
            iplong.append(il)
        if node_ip["ip"] in iphost:
            cur_iplong = il
            cur_ip = node_ip["ip"]
    iplong.sort()

    slave_data = []
    # 对指定节点分配分配任务至当前节点
    if cur_ip in assign_app:
        for app_id in assign_app[cur_ip]:
            slave_data += all_data_by_app[app_id]
    else:
        # 对未指定节点的任务分配任务至当前节点
        for t in data:
            # 构建一个循环先进先出的节点队列，用于接收任务
            accept_host = iplong[0]
            del iplong[0]
            iplong.append(accept_host)
            if cur_iplong == accept_host:
                if isbyapp:
                    slave_data += all_data_by_app[t]
                else:
                    slave_data.append(t)

    return slave_data


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