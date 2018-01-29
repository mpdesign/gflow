# -*- coding: utf-8 -*-
# Filename: demoJob.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-07-26
# Author:       mpdesign
# description:  作业控制器，一个作业包含多个任务
# -----------------------------------

from work.hdfs.hdfs import *


class demoJob(hdfsLayer):
    def __init__(self):

        hdfsLayer.__init__(self)
        # 任务名注册表
        self.registerTask = [
            'mapper',
            # 可设置多层reducer, 任务名以reducer开头
            'reducer',
            'reducer2'
        ]

    def hdfsConfig(self):
        return {
            # 'files': ['/home/hadoop/test.log'],
            # 文件夹的文件列表，则末尾需加上/
            'files': ['/home/hadoop/api_dc.access_log', '/home/hadoop/api_dc.access_log'],
            # if block_file：block_num=count(block_file)
            # if block_size: block_num=len(files)/block_size，默认128M
            'block_file': True,
            'block_num': 10,
            'block_size': 0,
            'delimiter': '\n',
            'encoding': 'utf8',
            # 不设置上传线程数，则默认为分配到的任务数据列表长度
            'uploadThreadNum': 0,
            # 每个线程处理的任务数 num node all，该参数决定最终处理结果集的输出方式：node 由各节点各自汇总输出；all 由master汇总输出；num 由线程输出
            # num：分别处理num个任务数据 myTaskDataList/threadNum
            # node：汇总处理当前节点分配到的任务数据 myTaskDataList
            # all：汇总处理集群全部任务数据 taskDataList
            # 不设置，则系统自动分配
            'mapperExcuteTaskNumBy': 1,
            'reducerExcuteTaskNumBy': 'node',
            'reducer2ExcuteTaskNumBy': 'all'
        }