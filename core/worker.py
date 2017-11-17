# -*- coding: utf-8 -*-
# Filename: worker.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-11-06
# Author:       mpdesign
# description:  进程/线程脚本
# -----------------------------------

import Queue
import threading
import multiprocessing
from comm.common import *


class WorkerManager:
    def __init__(self, num_of_workers=10, PT='t'):
        #未开辟新进程之前获取父进程pid
        self.w_pf = "%s/tmp/pids/%s.pid" % (PATH_CONFIG["project_path"], os.getpid())
        self.workQueue = None
        self.resultQueue = None
        self.workers = []
        self.wqtimeout = None
        self._recruitThreads(num_of_workers, PT)

    def _recruitThreads(self, num_of_workers, PT='t'):
        if PT == 't':
            self.workQueue = Queue.Queue()
            self.resultQueue = Queue.Queue()
        else:
            #进程队列，传入的数据必须可pickle
            self.workQueue = multiprocessing.Queue()
            self.resultQueue = multiprocessing.Queue()
        for i in range(num_of_workers):
            if PT == 't':
                worker = Threader(self.workQueue, self.resultQueue)
            else:
                worker = Processer(self.workQueue, self.resultQueue)
            worker.w_pf = self.w_pf
            self.workers.append(worker)

    def aliveWorkers(self):
        i = 0
        for worker in self.workers:
            if worker.isAlive():
                i += 1
        return i

    # 串行执行
    def serial_for_complete(self):
        # ...then, wait for each of them to terminate:
        for worker in self.workers:
            # worker = self.workers.pop()
            worker.start()
            worker.join()
            # if worker.isAlive() and not self.workQueue.empty():
            #     self.workers.append(worker)
        return self

    # 并行执行
    def parallel_for_complete(self):
        for worker in self.workers:
            worker.start()
        return self

    # 添加任务队列
    def add(self, _callable, *args, **kwds):
        self.workQueue.put((_callable, args, kwds))
        return self

    #  返回任务结果队列
    def result(self, rqtimeout=None):
        try:
            res = self.resultQueue.get(timeout=rqtimeout)
        except Exception, e:
            return 'timeout'
        return singleton.getinstance('pjson').loads(res)

    # sleeptime后终止线程
    def stop(self, _stop=True, sleeptime=0, afterwork=True):
        for worker in self.workers:
            worker.stop(_stop, afterwork)
        if sleeptime > 0:
            time.sleep(sleeptime)
        return self

    # 队列超时时间
    def qtimeout(self, _timeout):
        if _timeout > 0:
            for worker in self.workers:
                worker.timeout(_timeout)
        return self


class Processer(multiprocessing.Process):

    def __init__(self, workQueue, resultQueue, **kwds):
        multiprocessing.Process.__init__(self, **kwds)
        self.workQueue = workQueue
        self.resultQueue = resultQueue
        self.worker_stop = False
        self.w_pf = ''
        self.wqtimeout = None

    def run(self):
        self.savepid()
        while True:
            try:
                _callable, args, kwds = self.workQueue.get(timeout=self.wqtimeout)
            except Exception, e:
                output(('pQueue: ', _callable, args, kwds, e))
                time.sleep(10)
                continue

            # stop by workQueue
            if "stop" in kwds.keys():
                break
            if "loop" in kwds.keys() and kwds["loop"]:

                self.whiledo(_callable, *args, **kwds)

            else:
                res = self.do(_callable, *args, **kwds)
                res = singleton.getinstance('pjson').dumps(res)
                self.resultQueue.put(res)
            # stop by flag
            if self.worker_stop:
                break

        try:
            sys.exit(0)
        except Exception, e:
            pass

    @staticmethod
    def do(_callable, *args, **kwds):
        # _callable must be pickle
        if "obj" in kwds.keys() and kwds["obj"]:
            obj = kwds["obj"]
            kargs = kwds.copy()
            del kargs["obj"]
            return getattr(obj,  _callable)(*args, **kargs)
        else:
            return _callable(*args, **kwds)

    def whiledo(self, _callable, *args, **kwds):
        obj = None
        if "obj" in kwds.keys() and kwds["obj"]:
            obj = kwds["obj"]
            kargs = kwds.copy()
            del kargs["obj"]
        while not self.worker_stop:
            # _callable must be pickle
            if obj:
                getattr(obj,  _callable)(*args, **kargs)
            else:
                _callable(*args, **kwds)

    def stop(self, _stop=True, afterwork=True):
        if afterwork:
            self.worker_stop = _stop
        else:
            self.workQueue.put(("", [], {"stop": _stop}))
        return self

    def timeout(self, _timeout):
        self.wqtimeout = _timeout
        return self

    def isAlive(self):
        return self.is_alive()

    @staticmethod
    def getpid():
        return os.getpid()

    def savepid(self, w_pf=''):
        if not w_pf and self.w_pf:
            w_pf = self.w_pf
        file(w_pf, 'a+').write('%s\n' % self.getpid())


class Threader(threading.Thread):

    def __init__(self, workQueue, resultQueue, **kwds):
        threading.Thread.__init__(self, **kwds)
        #self.maxsize = 1000
        self.setDaemon(True)
        self.workQueue = workQueue
        self.resultQueue = resultQueue
        self.worker_stop = False
        self.w_pf = ''
        self.wqtimeout = None

    def run(self):
        while True:
            try:
                # 阻塞
                _callable, args, kwds = self.workQueue.get(timeout=self.wqtimeout)
            except Exception, e:
                output(('tQueue: ', _callable, args, kwds, e))
                time.sleep(10)
                continue
            # stop by one workQueue exp: self.workQueue.put(("", [], {"stop": _stop}))
            if "stop" in kwds.keys():
                break
            if "loop" in kwds.keys() and kwds["loop"]:
                del kwds["loop"]
                self.whiledo(_callable, *args, **kwds)
            else:
                res = self.do(_callable, *args, **kwds)
                res = singleton.getinstance('pjson').dumps(res)
                self.resultQueue.put(res)
            # stop by all flag exp: worker.stop()
            if self.worker_stop:
                break
        try:
            sys.exit(0)
        except Exception, e:
            pass

    @staticmethod
    def do(_callable, *args, **kwds):
        if _callable:
            return _callable(*args, **kwds)

    def whiledo(self, _callable, *args, **kwds):
        while not self.worker_stop:
            _callable(*args, **kwds)

    def stop(self, _stop=True, afterwork=True):
        if afterwork:
            self.worker_stop = _stop
        else:
            self.workQueue.put(("", [], {"stop": _stop}))
        return self

    def timeout(self, _timeout):
        self.wqtimeout = _timeout
        return self

    def getpid(self):
        return "%s-%s" % (self.name, os.getpid())

    def savepid(self, w_pf=''):
        pass

