# -*- coding: utf-8 -*-
# Filename: mp.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-07-11
# Author:       mpdesign
# description:  核心控制文件
# -----------------------------------

from comm.common import *
from worker import *
from results import *
from inspect import *
import math


class mp:

    def __init__(self):
        # 作业注册表
        self.registerJob = []

        """
        任务配置
        """
        # 任务注册表
        self.registerTask = []
        # 总任务数据列表
        self.__taskDataList = []
        # 当前节点分配到的任务数据列表
        self.__myTaskDataList = []
        # 需等待执行的任务
        self.waiteForTask = ''

        """
         结果集配置
        """
        # 当前节点任务结果集
        self.resultSet = []

        """
         Execute配置
        """
        # 是否跳出循环，单次执行
        self.breakExecute = False
        # 循环执行，间隔睡眠时间，单位秒
        self.sleepExecute = 0
        # 循环执行，指定时间点执行，单位m d H M S
        self.atExecute = ""
        # 是否可重跑, 0不可重跑、1只允许重跑一次、2允许重跑， self.atExecute 默认可重跑
        self.enableReExecute = 2
        # 每个线程处理的任务数 num node all
        # num：处理num个任务数据 myTaskDataList/threadNum
        # node：处理当前节点分配到的任务数据 myTaskDataList
        # all：处理集群全部任务数据 taskDataList
        self.excuteTaskNumBy = 10
        # 任务执行超时时间
        self.executeTimeout = None

        # 可分配的节点
        self.usedNode = []
        # 当前节点
        self.cur_node = curNode()

        # 检查项
        self.inspects = {}

        # 当前时间
        self._curTime = time.time()
        # 启动时间
        self.startTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

        pkg_job = argv_cli['argvs'][2].split('.')
        self.layerName = pkg_job[0] if len(pkg_job) > 1 else ''
        self.jobName = argv_cli['argvs'][2]
        self.taskName = self.__class__.__name__[0:-4]

    # 执行当前作业的所有注册任务
    def jobExecute(self):
        if len(self.registerTask) > 0:
            # 多线程执行作业
            self.__exec_job()
        else:
            output('Job %s has not registerTask' % self.jobName, logType='run')

    # 多线程执行当前作业下的所有注册任务
    def __exec_job(self):
        # 自定义任务
        if "t" in argv_cli["dicts"].keys() and argv_cli["dicts"]["t"]:
            self.registerTask = argv_cli["dicts"]["t"].split(",")
        task_len = len(self.registerTask)
        taskPool = WorkerManager(task_len)
        taskPool.parallel_for_complete().stop()
        work_num = 0
        for t in self.registerTask:
            newtask = self.__import_task(t)
            if not newtask:
                continue
            # 删除inspect文件
            inspect.delete('%s.%s' % (newtask.jobName, newtask.taskName))
            newtask.inspect(data={'executeStatus': '__sub_exec_task'})
            # breakExecute不加入监听队列
            newtask.beforeExecute()
            if not newtask.breakExecute:
                work_num += 1
            taskPool.add(self.__sub_exec_task, newtask)
        # 监听子线程
        if work_num == 0:
            while taskPool and taskPool.aliveWorkers() > 0:
                time.sleep(3)
            output('The job [%s] has been completed and is safely withdrawn from the process' % self.jobName, logType='run')
        else:
            if "d" in argv_cli["dicts"].keys() and argv_cli["dicts"]["d"]:
                while taskPool and taskPool.aliveWorkers() > 0:
                    time.sleep(3)
                output('The job [%s] has been rerun and is safely withdrawn from the process' % self.jobName, logType='run')
            else:
                while taskPool and taskPool.aliveWorkers() >= work_num:
                    time.sleep(60)
                notice_me('The job [%s] process has an abnormal exit of a subtask' % self.jobName)
                while taskPool and taskPool.aliveWorkers() > 0:
                    time.sleep(60)
                notice_me('The whole process of the job [%s] process exits' % self.jobName)
        sys.exit(0)


    # 执行子任务
    @staticmethod
    def __sub_exec_task(newtask):
        # 重跑
        if "d" in argv_cli["dicts"].keys() and argv_cli["dicts"]["d"]:
            return newtask.reExecute()
        else:
            return newtask.forExecute()

    # 按时间计划执行任务
    def forExecute(self):
        if "now" not in argv_cli['dicts'].keys():
            isnowtask = False
        else:
            isnowtask = True if not argv_cli['dicts']['now'] or self.taskName in argv_cli['dicts']['now'].split(',') else False
        isnowtask = isnowtask if self.enableReExecute > 0 else False
        isnow = True
        while True:
            self.beforeExecute()
            # 是否马上执行
            if not isnow or not isnowtask:
                # 间隔执行
                self.sleepExecute = intval(self.sleepExecute)
                if self.sleepExecute > 0:
                    time.sleep(self.sleepExecute)
                # 定时执行
                elif self.atExecute and len(self.atExecute) > 1:
                    left_time = self.__atTime(self.atExecute)
                    time.sleep(left_time)
                elif not self.breakExecute:
                    output('Please set Execute type: atExecute, breakExecute, sleepExecute ', taskName=self.taskName, logType='run')
                    break
            isnow = False
            self.curTime(curtime=time.time())
            # 等待其他任务执行完毕
            self.witeForComplete()
            # 重新准备执行任务
            self.prepareExecute()
            # 开始执行
            output('startExecute', taskName=self.taskName, logType='run')
            self.inspect(data={'executeStatus': 'startExecute'})
            resultSets = self.mutiThreadExecute()
            self.afterExecute(resultSets=resultSets)
            # 执行结束
            self.inspect(data={'executeStatus': 'endExecute'})
            output('endExecute', taskName=self.taskName, logType='run')

            # 完成
            if self.breakExecute:
                break

    # 重跑计划任务
    def reExecute(self):
        # 只针对定时任务，并且可重跑的任务，进行重跑数据
        if not self.atExecute or len(self.atExecute) < 2 or self.enableReExecute < 2:
            return None

        date_range = argv_cli["dicts"]["d"].split(',')
        if len(date_range) >= 2:
            date_start = intval(date_range[0])
            date_end = intval(date_range[1])
        else:
            date_start = intval(date_range[0])
            date_end = date_start
        date_start = str(date_start)
        date_end = str(date_end)

        timeType = self.atExecute[0:1]

        if timeType == 'm' and len(date_start) == 6 and len(date_end) == 6:
            date_start_time = mktime('%s-%s-01 00:00:00' % (date_start[0:4], date_start[4:6]))
            date_end_time = mktime('%s-%s-01 00:00:00' % (date_end[0:4], date_end[4:6]))
        #周补零至7位区分月份，如：2014010
        elif timeType == 'w' and len(date_start) == 7 and len(date_end) == 7:
            date_start_time = mktimew('%s-%s' % (date_start[0:4], date_start[4:6]))
            date_end_time = mktimew('%s-%s' % (date_end[0:4], date_end[4:6]))
        elif timeType == 'd' and len(date_start) == 8 and len(date_end) == 8:
            date_start_time = mktime('%s-%s-%s 00:00:00' % (date_start[0:4], date_start[4:6], date_start[6:8]))
            date_end_time = mktime('%s-%s-%s 00:00:00' % (date_end[0:4], date_end[4:6], date_end[6:8]))
        elif timeType == 'H' and len(date_start) == 10 and len(date_end) == 10:
            date_start_time = mktime('%s-%s-%s %s:00:00' % (date_start[0:4], date_start[4:6], date_start[6:8], date_start[8:10]))
            date_end_time = mktime('%s-%s-%s %s:00:00' % (date_end[0:4], date_end[4:6], date_end[6:8], date_end[8:10]))
        elif timeType == 'M' and len(date_start) == 12 and len(date_end) == 12:
            date_start_time = mktime('%s-%s-%s %s:%s:00' % (date_start[0:4], date_start[4:6], date_start[6:8], date_start[8:10], date_start[10:12]))
            date_end_time = mktime('%s-%s-%s %s:%s:00' % (date_end[0:4], date_end[4:6], date_end[6:8], date_end[8:10], date_end[10:12]))
        else:
            output('date range error', taskName=self.taskName)
            return None
        self.inspect(data={'executeStatus': 'startReExecute'})
        while True:
            self.beforeExecute()
            if date_start_time > date_end_time:
                break
            self.curTime(curtime=date_start_time)

            self.prepareExecute()
            resultSets = self.mutiThreadExecute()
            self.afterExecute(resultSets=resultSets)

            if timeType == 'm':
                date_start_time = monthtime(m=1, curtime=date_start_time)
            elif timeType == 'w':
                date_start_time += 7*24*3600
            elif timeType == 'd':
                date_start_time += 24*3600
            elif timeType == 'H':
                date_start_time += 3600
            elif timeType == 'M':
                date_start_time += 60
            else:
                break
            time.sleep(0.3)
        output('reExecute complete', taskName=self.taskName, logType='run')
        self.inspect(data={'executeStatus': 'completeReExecute'})
        # 重跑结束，自动退出

    # 多线程执行分配到的业务
    def mutiThreadExecute(self):
        if not self.__myTaskDataList:
            output('myTaskDataList is none', taskName=self.taskName, logType='run')
            return
        if not isinstance(self.__myTaskDataList, type([])):
            _myTaskDataList = []
        else:
            _myTaskDataList = self.__myTaskDataList[0:]

        # 按游戏过滤
        if len(_myTaskDataList) > 0 and isinstance(_myTaskDataList[0], type({})) and 'app_id' in _myTaskDataList[0].keys():
            gid = ''
            sid_strat = 0
            sid_end = 0
            cid = ''
            # 过滤游戏
            if "g" in argv_cli["dicts"].keys() and argv_cli["dicts"]["g"] != '':
                gid = str(argv_cli["dicts"]["g"])
            # 过滤服务器
            if "s" in argv_cli["dicts"].keys() and argv_cli["dicts"]["s"]:
                sid_range = argv_cli["dicts"]["s"].split(',')
                if len(sid_range) > 1:
                    sid_strat = intval(sid_range[0])
                    sid_end = intval(sid_range[1])
            # 过滤渠道
            if "c" in argv_cli["dicts"].keys() and argv_cli["dicts"]["c"] != '':
                cid = argv_cli["dicts"]["c"]

            tmp_task = []
            for t in _myTaskDataList:
                g_id = str(t['app_id'])
                if gid and g_id != gid:
                    continue
                s_id = intval(t['sid']) if 'sid' in t.keys() else 0
                if s_id > 0 and sid_strat > 0 and sid_end > 0 and (s_id < sid_strat or s_id > sid_end):
                    continue
                c_id = t['channel_id'] if 'channel_id' in t.keys() else ''
                if c_id != '' and cid != '' and c_id != cid:
                    continue
                tmp_task.append(t)

            _myTaskDataList = tmp_task
        resultSets = []
        task_len = len(_myTaskDataList)

        if task_len < 2 or self.excuteTaskNumBy in ['node', 'all']:
            resultSet = self._execute(myTaskDataList=_myTaskDataList)
            resultSets.append(resultSet)
        else:
            task_len = task_len if task_len > 1 else 1
            # 每10个业务开辟一个线程
            tnum = self.excuteTaskNumBy
            wnum = int(math.ceil(float(task_len)/float(tnum)))
            wnum = min(50, wnum)
            self.inspect(data={'taskNum': task_len, 'workerNum': wnum})
            # 开辟wnum个线程
            subTaskPool = WorkerManager(wnum)
            # 并行执行，发送线程终止信号，在业务执行完毕之后终止
            subTaskPool.parallel_for_complete()
            # 添加wnum个任务
            for i in range(0, wnum):
                if i == wnum-1:
                    mt = _myTaskDataList[i*tnum:]
                else:
                    mt = _myTaskDataList[i*tnum:i*tnum+tnum]
                subTaskPool.add(self._execute, myTaskDataList=mt)
            # 1S 内监控线程是否正常运行, 只要一个线程出现异常则退出当前所有业务
            timeSleepNum = 0
            while int(timeSleepNum) < 1:
                time.sleep(0.1)
                timeSleepNum += 0.1
                if subTaskPool.aliveWorkers() < wnum:
                    output('A business thread in the task is not normally executed, and the whole job process has withdrawn. Please check it', taskName=self.taskName, logType='run')
                    sys.exit(1)

            # 获取结果集后再停止任务线程
            subTaskPool.stop(afterwork=False)

            # 等待获取所有任务结果
            ci = ti = 0
            for i in range(0, wnum):
                resultSet = subTaskPool.result(rqtimeout=self.executeTimeout)
                if isinstance(resultSet, type('')) and resultSet == 'timeout':
                    ti += 1
                    self.inspect(data={'workerTimeoutNum': ti})
                    output('Task execution has gone out of time', taskName=self.taskName, logType='run')
                    notice_me('Task[%s] execution has gone out of time' % self.taskName)
                    # _exit(1)
                else:
                    ci += 1
                    self.inspect(data={'completedWorkerNum': ci})
                    resultSets.append(resultSet)
        # 收集resultSet
        if hasattr(self, 'gather'):
            resultSets = get_attr(self, 'gather')(resultSets=resultSets)
        resultSets = self.afterGather(resultSets=resultSets)
        return resultSets

    def afterGather(self, resultSets=None):
        return resultSets

    # 总任务数据列表, 用于分配任务到各个机器并行计算
    @staticmethod
    def taskDataList():
        return []

    # 当前节点的任务数据列表
    def myTaskDataList(self, taskDataList=None):
        _myTaskDataList, usedNode = distributeTaskData(data=taskDataList)
        self.usedNode = usedNode
        return _myTaskDataList

    # 执行任务前的准确
    def prepareExecute(self):
        self.__myTaskDataList = self.myTaskDataList(self.taskDataList())
        self.inspect(data={'executeStatus': 'prepareExecute'})
        if not self.__myTaskDataList:
            # 未分配到任务则僵死
            output('The assigned task data is empty', taskName=self.taskName, logType='run')
            if self.breakExecute:
                sys.exit(0)
            else:
                while True:
                    time.sleep(3600)

        # 执行时间最大不能超过: 指定值 or min(下次执行的计划时间点/2)
        self.__executeTimeOut()
        if self.cur_node not in self.usedNode:
            self.usedNode.append(self.cur_node)

    # default method
    def execute(self, myTaskDataList=[]):
        pass

    # exec beforeMapper execute() afterMapper()
    def _execute(self, myTaskDataList=[]):
        if hasattr(self, 'beforeMapper'):
            myTaskDataList = get_attr(self, 'beforeMapper')(myTaskDataList=myTaskDataList)
        elif hasattr(self, 'beforeReducer'):
            myTaskDataList = get_attr(self, 'beforeReducer')(myTaskDataList=myTaskDataList)
        resultSet = self.execute(myTaskDataList=myTaskDataList)
        if hasattr(self, 'afterMapper'):
            resultSet = get_attr(self, 'afterMapper')(resultSet=resultSet)
        elif hasattr(self, 'afterReducer'):
            resultSet = get_attr(self, 'afterReducer')(resultSet=resultSet)
        return resultSet

    def __import_task(self, taskName=''):
        if not taskName:
            output('%s is not exists' % taskName, logType='run')
            return None
        task_file = '%s/work/%s/%s.py' % (PATH_CONFIG['project_path'], self.jobName.replace('.', '/'), taskName)
        if singleton.getinstance('pfile').isfile(task_file):
            pkg_name = 'work.%s.%s' % (self.jobName, taskName)
        else:
            output('package file %s is not exists' % task_file, logType='run')
            return None
        import_task = "from %s import *" % pkg_name
        exec(import_task)
        task_class_name = '%sTask' % taskName
        task_class = eval(task_class_name)
        newtask = task_class()
        return newtask

    # 设置 获取当前时间
    def curTime(self, curtime=0):
        if curtime > 0:
            self._curTime = intval(curtime)
        if self._curTime < 1:
            self._curTime = intval(time.time())
        return self._curTime

    # 定时执行时间
    def __atTime(self, dateT):
        return atTime(dateT)

    # 执行超时时间，执行时间最大不能超过: 指定值 or min(下次执行的计划时间点/2, 2小时)
    def __executeTimeOut(self):
        if self.executeTimeout > 0:
            self.executeTimeout = self.executeTimeout
        elif self.atExecute and len(self.atExecute) > 1:
            self.executeTimeout = self.__atTime(self.atExecute)/2
        self.executeTimeout = self.executeTimeout if self.executeTimeout > 0 else None

    # beforeStart method
    def beforeStart(self):
        pass

    def afterStart(self):
        pass

    # beforeJob method
    def beforeJob(self):
        pass

    # afterJob method
    def afterJob(self):
        pass

    # beforeExecute method
    # 执行前的静态配置，建议不作逻辑判断
    def beforeExecute(self):
        pass

    # afterExecute method
    def afterExecute(self, resultSets=None):
        return resultSets

    def beforeStop(self):
        for t in self.registerTask:
            self.taskName = t
            self.inspect(data={'executeStatus': 'stopped'})

    def afterStop(self):
        pass

    # 检查运行状态
    # overwrite 是否覆盖key
    # append 对于list类型value是否追加方式
    def inspect(self, data={}, k='', ipnode='', taskFullName='', overwrite=True, append=False):
        uninspectval = ['_mp__taskDataList', '_mp__myTaskDataList', 'resultSet', 'inspects', '_curTime']
        # 获取当前运行状态
        #  更新self.inspects
        if not self.inspects:
            inspects = {name: value for name, value in vars(self).items() if name not in uninspectval}
            self.inspects.update(inspects)
        # 读取状态
        if not data:
            inspects = self.inspects
            # 只要节点不是当前节点 或者任务不是当前任务，则从文件读取
            if ipnode and ipnode != self.cur_node or (taskFullName and taskFullName != '%s.%s' % (self.jobName, self.taskName)):
                inspects = inspect.read(taskFullName=taskFullName, ipnode=ipnode)
            return inspects if not k else itemDict(inspects, k)
        # 写入/更新状态
        else:

            self.inspects['argv_cli'] = argv_cli
            self.inspects['taskTime'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.curTime()))
            self.inspects['runTime'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            self.inspects['pid'] = os.getpid()

            self.inspects['usedNode'] = self.usedNode
            self.inspects['myTaskDataList'] = str([(d['index'], itemDict(d, 'app_id')) for d in self.__myTaskDataList]) if self.__myTaskDataList else []

            for _k in data:
                if _k not in self.inspects:
                    self.inspects[_k] = data[_k]
                    continue
                if overwrite:
                    self.inspects[_k] = data[_k]
                else:
                    if isinstance(data[_k], type([])):
                        if append:
                            self.inspects[_k] += data[_k]
                        else:
                            for l in data[_k]:
                                if l not in self.inspects[_k]:
                                    self.inspects[_k].append(l)
                    elif isinstance(data[_k], type({})):
                        # 字典不可追加
                        d = self.inspects[_k].copy()
                        d.update(data[_k])
                        self.inspects[_k] = d
                    else:
                        self.inspects[_k] += data[_k]
            taskFullName = taskFullName or '%s.%s' % (self.jobName, self.taskName)
            inspect.write(taskFullName=taskFullName, data=self.inspects)

    # 查看指定任务是否已完成
    def witeForComplete(self):
        if not self.waiteForTask:
            return
        self.inspect(data={'executeStatus': 'witeForComplete'})
        # 查看日志
        i = 0
        j = 1
        while True:
            inspects = self.inspect(taskFullName=self.waiteForTask)
            if not inspects:
                break
            if not inspects['atExecute']:
                break
            taskTime = mktime(inspects['taskTime'])
            timeType = str(inspects['atExecute'])[0:1]
            # 已执行结束，并且任务时间是在当前周期内
            complete = False
            if inspects['executeStatus'] == 'endExecute':
                for dt in ['m', 'd', 'H', 'M']:
                    if timeType == dt and time.strftime('%s%s' % ('%', dt), time.localtime(taskTime)) == time.strftime('%s%s' % ('%', dt), time.localtime()):
                        complete = True
                        break
            if complete:
                break
            # 等待半小时将自动跳过，并发送短信通知
            if i > 1800:
                output('The waiting for the completion of the task[%s] is not yet completed, no longer waiting for' % self.waiteForTask, taskName=self.taskName, logType='run')
                notice_me('The waiting for the completion of the task[%s] is not yet completed, no longer waiting for' % self.waiteForTask)
                break

            if j > 3:
                j *= 2
            j += 1
            i += j
            time.sleep(j)
