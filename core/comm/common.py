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
# 命令行参数
# argv_cli = getargvs()


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
