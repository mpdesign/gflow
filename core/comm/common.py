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
from config import *
from cfunc import *
from cdate import *
from singleton import *


# 输出
def output(msg, file_name='', log_type='', task_name=''):
    if len(argv_cli['argvs']) >= 3:
        job_name = argv_cli['argvs'][2]
    elif len(argv_cli['argvs']) == 2:
        job_name = argv_cli['argvs'][1]
    else:
        job_name = argv_cli['argvs'][0]
    job_name = '%s.%s' % (job_name, task_name) if task_name else job_name
    if file_name or log_type:
        logger(msg, job_name=job_name, file_name=file_name, log_type=log_type)

    else:
        # 将影响后台运行的多线程，阻塞其运行
        # 标准输出
        sys.stdout.write("[%s @%s] %s\r\n" % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), job_name, str(msg)))


# 日志
def logger(msg, job_name='', file_name='', log_type=''):
    if not file_name:
        log_path = PATH_CONFIG["log_path"]
        date_path = time.strftime('/%Y/%m', time.localtime())
        file_path = '%s%s' % (log_path, date_path)
        if log_type:
            file_name = '%s/%s/%s.log' % (file_path, time.strftime('%d', time.localtime()), log_type)
        else:
            file_name = '%s/%s/stdout.log' % (file_path, time.strftime('%d', time.localtime()))
    try:
        msg = str(msg)
    except Exception, e:
        msg = 'logger msg type error'
    job_name = job_name if job_name else argv_cli['argvs'][2]
    msg = '[%s @%s] %s\r\n' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), job_name, msg)
    singleton.getinstance('pfile').set_file(file_name).write(msg)


# 通知
def notice_me(message):
    try:
        if len(argv_cli['argvs']) >= 3:
            job_name = argv_cli['argvs'][2]
        elif len(argv_cli['argvs']) == 2:
            job_name = argv_cli['argvs'][1]
        else:
            job_name = argv_cli['argvs'][0]
        ip2 = curNode()
        try:
            message = str(message)
        except Exception, e:
            message = 'notice message type error'
        message = "%s %s - [%s%s]" % (time.strftime('%m-%d %H:%M:%S', time.localtime()), message, job_name, ip2)
        singleton.getinstance('phttp').send_sms(SMS_CONFIG["to"], message)
        singleton.getinstance('phttp').send_mail(MAIL_CONFIG["to"], "", MAIL_CONFIG["name"], message)
    except Exception, e:
        output(("notice_me: ", e, message))


def sysConnRdb():
    return singleton.getinstance('redisdb', 'core.db.redisdb').conn(
        REDIS_CONFIG['sys']['host'],
        REDIS_CONFIG['sys']['port'],
        REDIS_CONFIG['sys']['db']
    )


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

    # 如果数据包含app_id, 则按游戏划分
    isbyapp = False
    # 剔除有指定节点的游戏，一款游戏可指定多个运算节点，一个运算节点可被多款游戏指定（不推荐）
    assign_app = {}
    if isinstance(data[0], type({})) and 'app_id' in data[0].keys():
        all_data_by_app = {}
        un_assign_app = {}
        for t in data:
            if 'assign_node' in t.keys() and t['assign_node']:
                # 指定多个运算节点
                ans = t['assign_node'].split(',')
                for an in ans:
                    if an not in assign_app:
                        assign_app[an] = {}
                    assign_app[an][t['app_id']] = 1
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
    node_ips = []
    # 剩余可分配的节点
    for node_ip in SLAVE_NODE:
        # 剔除指定节点
        if node_ip["ip"] not in assign_app:
            node_ips.append(node_ip["ip"])
        if node_ip["ip"] in iphost:
            cur_ip = node_ip["ip"]
    node_ips.sort()

    slave_data = []
    # 对指定节点分配分配任务至当前节点
    if cur_ip in assign_app:
        # 运行在当前节点的所有游戏指定的所有其他节点
        app_nodes = {}
        for _ip in assign_app:
            for app_id in assign_app[cur_ip]:
                if app_id in assign_app[_ip]:
                    if app_id not in app_nodes:
                        app_nodes[app_id] = {}
                    app_nodes[app_id][_ip] = 1
        for app_id in app_nodes:
            # 单节点
            if len(app_nodes[app_id]) == 1:
                slave_data += all_data_by_app[app_id]
            # 多节点，则平均分配任务至所有节点
            else:
                app_nodes[app_id].sort()
                for t in all_data_by_app[app_id]:
                    accept_host = app_nodes[app_id][0]
                    del app_nodes[app_id][0]
                    app_nodes[app_id].append(accept_host)
                    if cur_ip == accept_host:
                        slave_data.append(t)
        return slave_data
    else:
        # 对未指定节点的任务分配任务至当前节点
        for t in data:
            # 构建一个循环先进先出的节点队列，用于接收任务
            accept_host = node_ips[0]
            del node_ips[0]
            node_ips.append(accept_host)
            if cur_ip == accept_host:
                if isbyapp:
                    slave_data += all_data_by_app[t]
                else:
                    slave_data.append(t)

    return slave_data
