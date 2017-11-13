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
import math


class mp:

    def __init__(self):
        self.layerName = ''
        self.jobName = ''
        # 作业注册表
        self.registerJob = []
        # 等待N秒后强制终止作业
        self.waiteForStopJobTime = 0

        """
        任务配置
        """
        # 任务注册表
        self.registerTask = []
        # 映射任务列表
        self.__mapTask = []
        # 当前节点需运算的任务
        self.__myTask = []

        """
         结果集配置
        """
        # 当前节点任务结果集
        self.resultSet = []
        # 所有节点任务结果集
        self.resultSets = []
        # 是否处理结果集
        self.ifDoResultSets = False

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
        # execute 每个线程执行任务数myTask
        self.executeNumEachThread = 0
        # 任务执行超时时间
        self.executeTimeout = 0

        # 当前时间
        self._curTime = time.time()

    # 执行当前作业的所有注册任务
    def jobExecute(self):
        pkg_job = argv_cli['argvs'][2].split('.')
        if len(pkg_job) > 1:
            self.layerName = pkg_job[0]
        self.jobName = argv_cli['argvs'][2]

        if len(self.registerTask) > 0:
            # 多线程执行作业
            self.__exec_job()
        else:
            output('Job %s has not registerTask' % self.__class__.__name__, log_type='system')

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
            # breakExecute不加入监听队列
            if not newtask.breakExecute:
                work_num += 1
            taskPool.add(self.__sub_exec_task, newtask)
        # 监听子线程
        if work_num == 0:
            while taskPool and taskPool.aliveWorkers() > 0:
                time.sleep(3)
            output('作业 [%s] 已完成，并安全退出进程' % self.__class__.__name__, log_type='system')
        else:
            if "d" in argv_cli["dicts"].keys() and argv_cli["dicts"]["d"]:
                while taskPool and taskPool.aliveWorkers() > 0:
                    time.sleep(3)
                output('作业 [%s] 已重跑完成，并安全退出进程' % self.__class__.__name__, log_type='system')
            else:
                while taskPool and taskPool.aliveWorkers() >= work_num:
                    time.sleep(60)
                notice_me('作业 [%s] 进程有子任务异常退出，请复查' % self.__class__.__name__)
                while taskPool and taskPool.aliveWorkers() > 0:
                    time.sleep(60)
                notice_me('作业 [%s] 进程全部进程异常退出，请复查' % self.__class__.__name__)
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
            isnowtask = True if not argv_cli['dicts']['now'] or self.__class__.__name__ in argv_cli['dicts']['now'].split(',') else False
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
                    output('%s Please set Execute type: atExecute, breakExecute, sleepExecute ' % self.__class__.__name__, log_type='system')
                    break
            isnow = False
            self.curTime(curtime=time.time())
            # 开始执行
            output('startExecute %s' % self.__class__.__name__, log_type='system')
            # 重新准备执行任务
            self.prepareExecute()
            self.mutiThreadExecute()
            # 处理结果集
            self.doResultQueue()
            self.afterExecute()
            # 重置结果集
            self.resultSet = []
            self.resultSets = []
            # output('endExecute %s' % self.__class__.__name__, log_type='system')
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
            output('%s date range error' % self.__class__.__name__, log_type='system')
            return None

        while True:
            self.beforeExecute()
            if date_start_time > date_end_time:
                break
            self.curTime(curtime=date_start_time)

            self.prepareExecute()
            self.mutiThreadExecute()
            # 处理结果集
            self.doResultQueue()
            self.afterExecute()
            # 重置结果集
            self.resultSet = []
            self.resultSets = []

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
        output('%s complete' % self.__class__.__name__, log_type='system')
        # 重跑结束，自动退出


    # 多线程执行分配到的业务
    def mutiThreadExecute(self):
        if not self.__myTask:
            output('_myTask is none', log_type='system')
            return
        if not isinstance(self.__myTask, type([])):
            _myTask = []
        else:
            _myTask = self.__myTask[0:]
            self.__myTask = []

        # 按游戏过滤
        if len(_myTask) > 0 and isinstance(_myTask[0], type({})) and 'app_id' in _myTask[0].keys():
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
            for t in _myTask:
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

            _myTask = tmp_task

        task_len = len(_myTask)
        if task_len < 2:
            self.execute(_myTask)
            return
        task_len = task_len if task_len > 1 else 1
        # 每10个业务开辟一个线程
        tnum = self.executeNumEachThread if self.executeNumEachThread > 0 else 10
        wnum = int(math.ceil(float(task_len)/float(tnum)))
        wnum = min(50, wnum)
        # 开辟wnum个线程
        subTaskPool = WorkerManager(wnum)
        # 并行执行，发送线程终止信号，在业务执行完毕之后终止
        subTaskPool.parallel_for_complete()
        # 开始执行
        timeStart = time.time()
        # 添加wnum个任务
        for i in range(0, wnum):
            if i == wnum-1:
                mt = _myTask[i*tnum:]
            else:
                mt = _myTask[i*tnum:i*tnum+tnum]
            subTaskPool.add(self.execute, myTask=mt)

        # 1S 内监控线程是否正常运行, 只要一个线程出现异常则退出当前所有业务
        timeSleepNum = 0
        while int(timeSleepNum) < 1:
            time.sleep(0.1)
            timeSleepNum += 0.1
            if subTaskPool.aliveWorkers() < wnum:
                output('任务[%s]中某个业务线程未正常执行，整个作业进程已退出，请复查' % self.__class__.__name__, log_type='system')
                sys.exit(1)

        # 执行时间最大不能超过: 指定值 or min(下次执行的计划时间点/2, 2小时)
        executeTimeout = 0
        if self.executeTimeout > 0:
            executeTimeout = self.executeTimeout
        elif self.atExecute and len(self.atExecute) > 1:
            executeTimeout = self.__atTime(self.atExecute)/2
        elif self.sleepExecute > 0:
            executeTimeout = self.sleepExecute/2

        # 获取结果集后再停止任务线程
        subTaskPool.stop(afterwork=False)

        # 等待获取所有任务结果
        for i in range(0, wnum):
            res = subTaskPool.result(rqtimeout=executeTimeout)
            if isinstance(res, type('')) and res == 'timeout':
                notice_me('任务[%s]执行已超时，强制退出，请复查' % self.__class__.__name__)
                sys.exit(1)
            self.resultSet.append(res)

        timeSleepNum = time.time() - timeStart
        while subTaskPool.aliveWorkers():
            # 超时，则通知但不退出
            if timeSleepNum > executeTimeout > 0:
                notice_me('任务[%s]已执行超过下次执行时间点的一半时间，强制退出，请复查' % self.__class__.__name__)
                # sys.exit(1)
            time.sleep(5)
            timeSleepNum += 5
            if timeSleepNum > 7200:
                notice_me('任务[%s]执行超过2小时，强制退出，请复查' % self.__class__.__name__)
                sys.exit(1)

    # 默认任务映射表,支持ip,int,[],可重构
    @staticmethod
    def mapTask():
        return []

    # 默认任务分配方法[by ip host],可重构
    @staticmethod
    def myTask(task_list=None, ip=None):
        # 代码层级指定分析节点
        if ip:
            iphost = ipaddress()
            if ip in iphost:
                return True
            else:
                return None
        else:
            return distributeTaskData(task_list)

    # 用于分配任务到各个机器并行计算
    def prepareExecute(self):
        mapTask = self.mapTask()
        if mapTask:
            self.__mapTask = mapTask
        if self.__mapTask:
            if isinstance(self.__mapTask, type('')):
                self.__myTask = self.myTask(ip=self.__mapTask)
            else:
                self.__myTask = self.myTask(self.__mapTask)

            # 未分配到任务则僵死
            if not self.__myTask:
                output('%s 未分配到任务' % self.__class__.__name__, log_type='system')
                while True:
                    time.sleep(3600)
                sys.exit(0)
            # 超时时间
            self.__executeTimeOut()

    # default method
    def execute(self, myTask=[]):
        pass

    def __import_task(self, task_name=''):
        if not task_name:
            output('%s is not exists' % task_name, log_type='system')
            return None
        task_file = '%s/work/%s/%s.py' % (path_config['gf_path'], self.jobName.replace('.', '/'), task_name)
        if singleton.getinstance('pfile').isfile(task_file):
            pkg_name = 'work.%s.%s' % (self.jobName, task_name)
        else:
            output('package file %s is not exists' % task_file, log_type='system')
            return None
        import_task = "from %s import *" % pkg_name
        exec(import_task)
        task_class_name = '%sTask' % task_name
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
        executeTimeout = 0
        if self.executeTimeout > 0:
            executeTimeout = self.executeTimeout
        elif self.atExecute and len(self.atExecute) > 1:
            executeTimeout = self.__atTime(self.atExecute)/2
        elif self.sleepExecute > 0:
            executeTimeout = self.sleepExecute/2
        self.executeTimeout = executeTimeout

    # beforeStart method
    def beforeStart(self):
        pass

    def afterStart(self):
        pass

    # beforeExecute method
    def beforeExecute(self):
        pass

    # afterExecute method
    def afterExecute(self):
        pass

    # beforeStop 由/slave stop发起终止信号
    def beforeStop(self):
        if not self.jobName:
            return
        curip = curNode()
        conn = sysConnRdb()
        stop_key = 'gf_job_stop_%s' % self.jobName
        for t in self.registerTask:
            stop_task = '%s_%s' % (t, curip)
            conn.redisInstance().zadd(stop_key, stop_task, 1)

    # 子任务监听终止信号,自行决定监听位置
    def ifStop(self):
        if not self.jobName:
            return
        if sysConnRdb().redisInstance().zscore(
            'gf_job_stop_%s' % self.jobName,
            '%s_%s' % (self.__class__.__name__, curNode()),
            1
        ):
            sys.exit(1)

    def afterStop(self):
        sysConnRdb().redisInstance().delete('gf_job_stop_%s' % self.jobName)

    # 处理结果队列
    def doResultQueue(self):
        if not self.ifDoResultSets:
            return
        mpResult.doResultQueue(tname=self.__class__.__name__, mapTask=self.mapTask(), resultSet=self.resultSet, executeTimeout=self.executeTimeout)


# 结果处理类
class mpResult:

    # 处理结果队列
    @staticmethod
    def doResultQueue(tname='', mapTask='', resultSet=[], executeTimeout=0):

        # 处理节点个数
        count_node = len(slave_node)
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
        mpResult.__pushResultQueue(queue_key, resultSet)

        # 单节点运算
        if count_node == 1:
            # 由当前节点处理结果
            mpResult.__popResultQueue(queue_key, count_node, executeTimeout)
        # master 并行运算
        else:
            # 由master节点归并处理所有slave节点结果队列
            if isMaster():
                mpResult.__popResultQueue(queue_key, count_node, executeTimeout)

    @staticmethod
    def __pushResultQueue(queue_key, resultSet):
        conn = sysConnRdb()
        conn.redisInstance().rpush(
            queue_key,
            singleton.getinstance('pjson').dumps(resultSet)
        )

    @staticmethod
    def __popResultQueue(queue_key, count_node, executeTimeout):
        resultSets = []
        conn = sysConnRdb()
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
