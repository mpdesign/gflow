# -*- coding: utf-8 -*-
# Filename: results.py

# -----------------------------------
# Revision:         2.0
# Date:             2017-11-03
# Author:           mpdesign
# description:      任务结果处理
# -----------------------------------

from comm.common import *
conn = sysConnRdb()


class results:

    # 处理结果队列
    @staticmethod
    def doResultQueue(tname='', mapTask='', resultSet=[], executeTimeout=0):

        # 处理节点个数
        count_node = len(SLAVE_NODE)
        # 指定节点则为1
        if isinstance(mapTask, type('')):
            count_node = 1
        # 由master发起
        if 'pid' in argv_cli['dicts'].keys():
            uid = argv_cli['dicts']['pid']
        else:
            uid = os.getpid()
            count_node = 1

        queue_key = 'flow_result_%s_%s' % (tname, uid)

        # 入队
        results.__pushResultQueue(queue_key, resultSet)

        # 单节点运算
        if count_node == 1:
            # 由当前节点处理结果
            return results.__popResultQueue(queue_key, count_node, executeTimeout)
        # master 并行运算
        else:
            # 由master节点归并处理所有slave节点结果队列
            if isMaster():
                return results.__popResultQueue(queue_key, count_node, executeTimeout)

    @staticmethod
    def __pushResultQueue(queue_key, resultSet):

        conn.redisInstance().rpush(
            queue_key,
            singleton.getinstance('pjson').dumps(resultSet)
        )

    @staticmethod
    def __popResultQueue(queue_key, count_node, executeTimeout):
        resultSets = []
        for i in range(0, count_node):
            try:
                k, res = conn.redisInstance().blpop(
                    queue_key,
                    timeout=executeTimeout
                )
                res = singleton.getinstance('pjson').loads(res)
            except Exception, e:
                res = 'timeout@%s' % curNode()
                executeTimeout = 3
            resultSets.append(res)
        # 处理完毕，清洗队列
        keys = conn.redisInstance().keys('_'.join(queue_key.split('_')[0:-1]) + '*')
        for k in keys:
            conn.redisInstance().delete(k)
        return resultSets