# -*- coding: utf-8 -*-
# Filename: client.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-11-06
# Author:       mpdesign
# description:  客户端信息
# -----------------------------------

import sys
import os
from config.config import *
import socket


# ---------------------
#获取当前机器ip eth=3 取全部IP eth=0 取当前IP
# ---------------------
def ipaddress(eth=3):
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


# 获取终端输入参数
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
