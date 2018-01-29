# -*- coding: utf-8 -*-
# Filename: dataInterface.py

# -----------------------------------
# Revision:         2.0
# Date:             2017-11-15
# Author:           mpdesign
# frequency:        timely
# -----------------------------------

from data import *
import dataModel


class dataInterface(dataJob):

    def __init__(self):

        setattr(self, "%sPool" % self.__class__.__name__, None)
        setattr(self, "%sNum" % self.__class__.__name__, 0)
        workInterface.__init__(self)

    def beforeExecute(self):
        self.sleepExecute = 300

    def taskDataList(self):
        return self.assignTask(byserver=False)

    # 每分钟定时监听新游戏，分配线程
    def mutiWorker(self, myTask=[], schemes={}, popKeyPre=''):
        workerNum = len(myTask)
        if workerNum < 1:
            return

        addWorker = False

        # 线程为空则创建
        if not getattr(self, "%sPool" % self.__class__.__name__):
            setattr(self, "%sNum" % self.__class__.__name__, workerNum)
            setattr(self, "%sPool" % self.__class__.__name__, WorkerManager(workerNum))
            getattr(self, "%sPool" % self.__class__.__name__).parallel_for_complete()
            addWorker = True

        # 上次创建的线程不够本次任务分配, 或者有线程退出，则停止旧线程重新创建新线程
        if getattr(self, "%sNum" % self.__class__.__name__) < workerNum:
            # 停止
            getattr(self, "%sPool" % self.__class__.__name__).stop()
            setattr(self, "%sNum" % self.__class__.__name__, workerNum)

            # 创建
            while getattr(self, "%sPool" % self.__class__.__name__).aliveWorkers() > 0:
                time.sleep(1)

            setattr(self, "%sPool" % self.__class__.__name__, WorkerManager(workerNum))
            getattr(self, "%sPool" % self.__class__.__name__).parallel_for_complete()
            addWorker = True

        if addWorker:
            for t in myTask:
                app_id = t['app_id']
                popKey = "%s_%s" % (popKeyPre, app_id)
                getattr(self, "%sPool" % self.__class__.__name__).add(self.whileWorker, popKey, schemes, loop=True)

        # 有线程退出，则发送通知
        if getattr(self, "%sPool" % self.__class__.__name__).aliveWorkers() < getattr(self, "%sNum" % self.__class__.__name__):
            # notice_me('线程%s@r2m已退出，请重启' % self.__class__.__name__)
            # 标记线程数，通知重启
            setattr(self, "%sNum" % self.__class__.__name__, getattr(self, "%sPool" % self.__class__.__name__).aliveWorkers())

    # 循环处理数据，是否还有更多数据在队列未处理
    def whileWorker(self, popKey, schemes):
        self.doWorker(popKey, schemes)

    @staticmethod
    def doWorker(popKey, schemes):

        # 取出数据
        rows_insert, rows_update = dataModel.popData(popKey, schemes)
        # 处理数据
        dataModel.doData(rows_insert, rows_update, schemes)
