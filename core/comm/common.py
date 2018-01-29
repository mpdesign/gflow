# -*- coding: utf-8 -*-
# Filename: common.py

# -----------------------------------
# Revision:     2.0
# Date:         2014-06-21
# Author:       mpdesign
# description:  公共脚本
# -----------------------------------

import sys
reload(sys)
sys.setdefaultencoding('utf8')
import os
import time
from conf import *
from cfunc import *
from cdate import *
from client import *
from singleton import *


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


# 输出
def output(msg, filePath='', logType='', taskName=''):
    if len(argv_cli['argvs']) >= 3:
        jobName = argv_cli['argvs'][2]
    elif len(argv_cli['argvs']) == 2:
        jobName = argv_cli['argvs'][1]
    else:
        jobName = argv_cli['argvs'][0]
    jobName = '%s.%s' % (jobName, taskName) if taskName else jobName
    if filePath or logType:
        logger(msg, jobName=jobName, filePath=filePath, logType=logType)

    else:
        # 将影响后台运行的多线程，阻塞其运行
        # 标准输出
        sys.stdout.write("[%s @%s] %s\r\n" % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), jobName, str(msg)))


# 日志
def logger(msg, jobName='', filePath='', logType=''):
    if not filePath:
        log_path = PATH_CONFIG["log_path"]
        date_path = time.strftime('/%Y/%m', time.localtime())
        filePath = '%s%s' % (log_path, date_path)
        if logType:
            filePath = '%s/%s/%s.log' % (filePath, time.strftime('%d', time.localtime()), logType)
        else:
            filePath = '%s/%s/stdout.log' % (filePath, time.strftime('%d', time.localtime()))
    try:
        msg = str(msg)
    except Exception, e:
        msg = 'logger msg type error'
    jobName = jobName if jobName else argv_cli['argvs'][2]
    msg = '[%s @%s] %s\r\n' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), jobName, msg)
    singleton.getinstance('pfile').set_file(filePath).write(msg)


# 通知
def notice_me(message):
    try:
        if len(argv_cli['argvs']) >= 3:
            jobName = argv_cli['argvs'][2]
        elif len(argv_cli['argvs']) == 2:
            jobName = argv_cli['argvs'][1]
        else:
            jobName = argv_cli['argvs'][0]
        ip2 = curNode()
        try:
            message = str(message)
        except Exception, e:
            message = 'notice message type error'
        message = "%s %s - [%s%s]" % (time.strftime('%m-%d %H:%M:%S', time.localtime()), message, jobName, ip2)
        singleton.getinstance('phttp').config_sms(SMS_CONFIG).send_sms(SMS_CONFIG["to"], message)
        singleton.getinstance('phttp').config_mail(MAIL_CONFIG).send_mail(MAIL_CONFIG["to"], "", MAIL_CONFIG["name"], message)
    except Exception, e:
        output(("notice_me: ", e, message), logType='stderr')


# 动态分发任务数据，列表类型
# 任务类型必须一致
# return [
#  {"data": [], "assign_node": [], "app_id": "", index: ""},
# ]
def distributeTaskData(data=[]):
    if not data:
        return None, 0

    if not isinstance(data, type([])):
        output('core.comm.common.distributeTaskData(data=[]) data type must be list', logType='run')
        _exit(0)
    if not isinstance(data[0], type({})) :
        output('core.comm.common.distributeTaskData(data=[]) ex: data = [{}]', logType='run')
        _exit(0)

    # 包含app_id则按游戏重新封装列表
    if isinstance(data[0], type({})) and 'app_id' in data[0] and not 'data' in data[0]:
        app_data = {}
        for t in data:
            if t['app_id'] not in app_data:
                app_data[t['app_id']] = []
            app_data[t['app_id']].append(t)
        data = []
        for app_id in app_data:
            assign_node = ''
            if app_data[app_id] and app_data[app_id][0] and 'assign_node' in app_data[app_id][0]:
                assign_node = app_data[app_id][0]['assign_node']
            for k,v in enumerate(app_data[app_id]):
                if 'assign_node' in v:
                    del app_data[app_id][k]['assign_node']
            if assign_node:
                tmp = {'app_id': app_id, 'assign_node':assign_node, 'data': app_data[app_id]}
            else:
                tmp = {'app_id': app_id, 'data': app_data[app_id]}
            data.append(tmp)

    # 剔除有指定节点的任务列表，一个任务列表可指定多个运算节点，一个运算节点可被多个任务列表指定（不推荐），已被指定的节点不能再接受其他任务列表派发

    # 已指定节点的任务列表
    assigned_data = []
    # 未指定
    unassign_data = []
    # 已被指定的节点
    assigned_node = {}
    # 按index字段排序
    if 'index' in data[0]:
        data = sorted(data, key=lambda d: intval(d['index']))
    for i in range(0, len(data)):
        if 'index' not in data[i]:
            data[i]['index'] = i
        if 'assign_node' in data[i]:
            # assign_node 转化为list类型
            an = data[i]['assign_node']
            data[i]['assign_node'] = an if isinstance(an, type([])) else an.split(',')
            # 指定多个运算节点
            for an in data[i]['assign_node']:
                assigned_node[an] = 1
            if 'data' in data[i] and isinstance(data[i]['data'], type([])):
                for j in range(0, len(data[i]['data'])):
                    data[i]['data'][j]['index'] = '%s-%s' % (data[i]['index'], j)
            assigned_data.append(data[i])
        else:
            unassign_data.append(data[i])
    del data
    # 剩余未分配的节点
    unassign_node = []
    # 剩余可分配的节点
    for node_ip in SLAVE_NODE:
        # 剔除指定节点
        if node_ip["ip"] not in assigned_node:
            unassign_node.append(node_ip["ip"])
    unassign_node.sort()

    # 当前节点可分配的任务列表, 为保证输出数据有序，转化后的任务列表有序（元组首个元素为索引）
    my_data = []

    # 当前节点ip
    cur_node = curNode()

    # 被分配使用的节点
    used_node = {}

    # --------------------
    # 分配完一个节点的所有任务再继续分配下一个节点，保证任务列表有序
    def avg_data_node(_data=[], _node=[]):
        _my_data = []
        if not _data:
            return _my_data
        _len_data = len(_data)
        _len_node = len(_node)
        avgnum_each_node = _len_data/_len_node
        leftnum = _len_data % _len_node
        _range_data_index = []
        offset = 0
        for i in range(0, _len_data):
            length = avgnum_each_node + 1 if leftnum > 0 else avgnum_each_node
            endset = offset + length
            if i == endset-1:
                _range_data_index.append(endset)
                offset += length
                leftnum -= 1

        # 分配到的节点
        for i in range(0, len(_range_data_index)):
            used_node[_node[i]] = 1

        if cur_node in _node:
            _cur_node_index = _node.index(cur_node)
            _my_data = _data[_range_data_index[_cur_node_index - 1] if _cur_node_index - 1 >= 0 else 0: _range_data_index[_cur_node_index]]
        return _my_data

    # 指定节点任务列表
    for i in range(0, len(assigned_data)):
        t = assigned_data[i]
        if len(t['assign_node']) > 1:
            if 'data' not in t or not isinstance(t['data'], type([])):
                output('data must be list when assign_node more than 1', logType='run')
                _exit(0)
            # 包含多个运算节点，则对t再划分任务列表，取当前节点的任务列表
            t['assign_node'].sort()
            my_data += avg_data_node(t['data'], t['assign_node'])
        elif cur_node == t['assign_node'][0]:
            my_data.append(t)
    # 未指定的任务列表则分配至unassign_node

    my_data += avg_data_node(unassign_data, unassign_node)

    return my_data, used_node.keys()



