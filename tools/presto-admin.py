#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 2018-02-01
@author: mpdesign
@File: presto/bin/presto-admin.py
"""
from fabric.api import run, env, local
from fabric.decorators import roles
from fabric.operations import put
from fabric.tasks import execute
import os
import sys

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

# ========== Config Begin ==========
coordinatorHosts = ['hadoop-master']
workerHosts = argv_cli['dicts']['worker'].split(',') if 'worker' in argv_cli['dicts'] else ['hadoop-slave1', 'hadoop-slave2']

sshUser = 'hadoop'

# 项目路径
presto_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


# ========== Config End ==========

env.user = sshUser
env.roledefs = {
    'coordinator': coordinatorHosts,
    'worker': workerHosts,
    'allHost': coordinatorHosts + workerHosts
}


@roles('worker')
def deployWrker():
    if local('hostname', capture=True) != env.host:
        run('mkdir -p ' + presto_path)
        put(presto_path, presto_path)



@roles('worker')
def reloadCatalog():
    run('rm -rf ' + presto_path + '/etc/catalog')
    put(presto_path + '/etc/catalog', presto_path + '/etc')


@roles('allHost')
def startAll():
    run(presto_path + '/bin/launcher start')


@roles('allHost')
def stopAll():
    run(presto_path + '/bin/launcher stop')


# ============ Avaliable methods as follow ============

def deploy():
    execute(deployWrker)


def start():
    execute(startAll)


def stop():
    execute(stopAll)


def restart():
    execute(stopAll)
    execute(startAll)


def reload():
    execute(reloadCatalog)
    execute(stopAll)
    execute(startAll)


# 操作列表
action = {'start': start, 'stop': stop, 'restart': restart, 'reload': reload}


# 执行动作
def switchAction(a):
    if a not in action.keys():
        exit("action '%s' is invalid!" % a)
    else:
        action.get(a)()
        # print '\n'


if __name__ == '__main__':
    switchAction(argv_cli['argvs'][1])

