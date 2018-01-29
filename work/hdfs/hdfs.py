# -*- coding: utf-8 -*-
# Filename: hdfs.py

# -----------------------------------
# Revision:         2.0
# Date:             2018-01-08
# Author:           mpdesign
# -----------------------------------
# 执行流
# > self.beforeExecute()
# > self.taskDataList()
# >> callback self.distributeMapper()
# > resultSets = self.mutiThreadExecute() 多线程
# >> self.beforeMapper()
# >> self.execute() callback line = self.mapper(line) return resultIndex, resultSet
# >> self.afterMapper((resultIndex, resultSet)) return resultIndex, resultSet
# > resultSets = [(resultIndex, resultSet), (resultIndex, resultSet), ...]
# > resultSets = self.gather(resultSets=resultSets) 用户聚合数据
# > self.afterGather(resultSets=resultSets) >> write hdfs
# > self.afterExecute()
# -----------------------------------

from work.__work__ import *


class hdfsLayer(workInterface):
    def __init__(self):
        workInterface.__init__(self)
        self.registerJob = [
            'demo'
        ]
        self.hconfig = {}
        self.hfolder = ''
        self.compressExt = ['.zip', '.gz']

    def beforeExecute(self):
        self.hconfig = self.hdfsConfig()
        if not self.hconfig['files']:
            output('hconfig error: unconfig ', logType='stderr')
            time.sleep(3600)
        self.hfolder = '%s/work/%s/%s-%s' % (PATH_CONFIG['hdfs_home'], self.jobName,  os.path.basename(self.hconfig['files'][0])[0:12], md5(singleton.getinstance('pjson').dumps(self.hconfig['files']))[0:6])
        if self.taskName == 'mapper':
            self.breakExecute = True
        elif self.taskName[0:7] == 'reducer':
            self.sleepExecute = 1
        if '%sExcuteTaskNumBy' % self.taskName in self.hconfig:
            self.excuteTaskNumBy = self.hconfig['%sExcuteTaskNumBy' % self.taskName]

        # 测试
        # if self.taskName == 'mapper':
        #     lines = []
        #     start = time.time()
        #     # a = 'hdfs_path – Target HDFS path. If it already exists and is a directory, files will be '
        #     # rang_i = 134217634/len(a)
        #     # output('test start', logType='hdfs')
        #     # for i in range(0, rang_i):
        #     #     lines.append(a)
        #     # output('test read completed used time %s' % (time.time() - start), logType='hdfs')
        #     # 测试读性能
        #     lines = self.readHdfs(filepath='/home/hadoop/test.log', offset=0, length=134217642)
        #     remote_file = '/gflow/work/hdfs.demo/0064e2d13c8f7ae73215d9612510dbab/mapper/parts-0'
        #     # 测试写入本地性能
        #     local_file = self.writeLocal(lines=lines, filepath=remote_file)
        #     # 测试上传性能
        #     self.uploadHdfs(remote_file=remote_file, local_file=local_file)
        # time.sleep(10000)

    # 任务分发列表
    def taskDataList(self):
        output(self.taskName + ' waite', logType='stdout')
        # 监听上一层任务是否完成, 未完成则继续等待
        self.inspect(data={"mrstep": 'beforeExecute.completedPrevHdfsTask'})
        status = self.completedPrevHdfsTask()
        # 当前任务在运行中，则不再继续重复执行
        if status == 'currrent_task_run':
            if self.taskName == self.registerTask[-1]:
                output('All task completed', logType='hdfs')
            while True:
                time.sleep(3600)
        else:
            while True:
                # 等待上一个任务执行完毕，才能执行当前任务
                if not status:
                    time.sleep(3)
                    status = self.completedPrevHdfsTask()
                else:
                    break
        output(self.taskName + ' pass', logType='stdout')
        # 开始
        self.inspect(data={"mrstep": 'mapTask.distribute', "mrcompleted": 0})

        if self.taskName == 'mapper':
            return self.distributeMapper()
        elif self.taskName[0:7] == 'reducer':
            return self.distributeReducer()

    # 默认按文件块大小分割，可自定义块数block_num
    def distributeMapper(self):
        blockList = []
        for f in self.hconfig['files']:
            if f[-1] == '/':
                try:
                    fs = singleton.getinstance('phdfs', 'core.db.phdfs').list(f)
                    for _f in fs:
                        blockList.append({'filepath': f + _f, 'offset': 0, 'length': None})
                except Exception,e:
                    output('distributeMapper list error %s' % str(e), logType='hdfs')
            elif sum([f[-len(i):] == i for i in self.compressExt]):
                blockList.append({'filepath': f, 'offset': 0, 'length': None})
            else:
                bs = singleton.getinstance('phdfs', 'core.db.phdfs')\
                    .setfile(f, delimiter=self.hconfig['delimiter'], encoding=self.hconfig['encoding'])\
                    .blockList(block_num=self.hconfig['block_num'])
                blockList += bs
        return blockList

    def distributeReducer(self):
        # 根据计算节点slavenode个数和文件块数来决定reducer运算深度
        reduceList = []
        prev_ask = self.registerTask[self.registerTask.index(self.taskName)-1]
        # 获取上一层级的任务(文件) prev_ask
        prev_task_ffolder = '%s/%s/' % (self.hfolder, prev_ask)
        prev_ask_files = singleton.getinstance('phdfs', 'core.db.phdfs').client().list(prev_task_ffolder)
        if not prev_ask_files:
            return []
        # 排序列表
        for f in prev_ask_files:
            idx = f.split('-')[1]
            fp = prev_task_ffolder + f
            # reducer 方案： node 由各节点各自汇总输出；all 由master汇总输出；num 由线程输出
            if self.excuteTaskNumBy == 'all':
                reduceList.append({'assign_node': DEFAULT_NODE, 'filepath': fp, 'offset': 0, 'length': None, 'index': idx})
            else:
                reduceList.append({'filepath': fp, 'offset': 0, 'length': None, 'index': idx})
        return reduceList

    # return resultIndex, resultSet
    def execute(self, myTaskDataList=[]):
        resultIndex = 0
        if self.taskName == 'mapper':
            resultSet = []
            for t in myTaskDataList:
                resultSet += self.readLines(filepath=t['filepath'], offset=t['offset'], length=t['length'], callbackProcess='mapper')
                resultIndex = t['index']
        else:
            resultSet = {}
            for t in myTaskDataList:
                resultSet = self.readLines(filepath=t['filepath'], callbackProcess='reducer', resultSet=resultSet)
                resultIndex = t['index']

            # 字典转化为list
            resultSet = ['%s %s' % (k, resultSet[k]) for k in resultSet]

        return resultIndex, resultSet

    def afterGather(self, resultSets=None):
        self.inspect(data={"mrstep": 'afterGather.writeHdfs'})
        if not resultSets:
            return
        if isinstance(resultSets, type(())):
            resultSets = [resultSets]
        upload_files = []
        for idx, ret in resultSets:
            remote_file = '%s/%s/parts-%s' % (self.hfolder, self.taskName, idx)
            local_file = self.writeLocal(lines=ret, filepath=remote_file, delimiter=self.hconfig['delimiter'])
            upload_files.append((remote_file, local_file))
        # 最多不超过20个并发
        uploadThreadNum = min(20, self.hconfig['uploadThreadNum'] or len(resultSets))
        # 开辟threadNum个线程
        gatherPool = WorkerManager(uploadThreadNum)
        # 并行执行，发送线程终止信号，在业务执行完毕之后终止
        gatherPool.parallel_for_complete()
        for remote_file, local_file in upload_files:
            gatherPool.add(self.uploadHdfs, remote_file, local_file)
        gatherPool.stop(afterwork=False)
        while gatherPool.aliveWorkers():
            time.sleep(3)
        # 记录完成状态
        self.inspect(data={"mrcompleted": 1})

    def readLines(self, filepath='', offset=0, length=None, buffer_size=None, delimiter='\n', encoding='utf-8', callbackProcess='mapper', resultSet={}):
        if hasattr(self, callbackProcess):
            callbackProcess = eval('self.' + callbackProcess)
        else:
            callbackProcess = eval(callbackProcess)
        # 如果filepath压缩文件则从hdfs下载至本地解压再读取
        if sum([filepath[-len(i):] == i for i in self.compressExt]):
            # 下载
            local_file_c = self.downloadHdfs(filepath)
            # 解压
            if filepath[-4:0] == '.zip':
                compressCmd = 'unzip'
                local_file = local_file_c[0:-4]
            else:
                compressCmd = 'gunzip'
                local_file = local_file_c[0:-3]
            os.system('%s %s' % (compressCmd, local_file))

            return self.readLocal(filepath=local_file, encoding=encoding, callbackProcess=callbackProcess, resultSet=resultSet)
        # 否则 从hdfs读取
        else:
            return self.readHdfs(filepath=filepath, offset=offset, length=length, buffer_size=buffer_size,
                                 delimiter=delimiter, encoding=encoding, callbackProcess=callbackProcess, resultSet=resultSet)

    # 读取文件内容，一次性读取，避免连接数过多; 读操作属于IO密集型，并发（协同开销）下载速度会小于队列下载
    def readHdfs(self, filepath='', offset=0, length=None, buffer_size=None, delimiter='\n', encoding='utf-8', callbackProcess=None, resultSet={}):

        start = time.time()
        output('read remote file: %s?offset[%s],length[%s] start' % (filepath, offset, length), logType='hdfs')
        try:
            if isinstance(resultSet, type([])):
                result = []
                with singleton.getinstance('phdfs', 'core.db.phdfs').client().read(filepath, offset=offset, length=length,
                                        buffer_size=buffer_size,
                                        delimiter=delimiter,
                                        encoding=encoding) as reader:
                    # 注意：必须加上 delimiter='\n', encoding='utf8'，否则需要加上r.read()，不然读取速度会很慢
                    for line in reader:
                        # 转换成字节码，加速运算
                        if isinstance(line, type(u'')):
                            line = line.decode('string-escape')
                        y = callbackProcess(x=line)
                        if y is not None:
                            result.append(y)
            else:
                result = resultSet
                with singleton.getinstance('phdfs', 'core.db.phdfs').client().read(filepath, offset=offset, length=length,
                                    buffer_size=buffer_size,
                                    delimiter=delimiter,
                                    encoding=encoding) as reader:
                    for line in reader:
                        # 转换成字节码，加速运算
                        if isinstance(line, type(u'')):
                            line = line.decode('string-escape')
                        if result is not None:
                            result = callbackProcess(x=line, y=result)

        except Exception, e:
            output(('readHdfs', e), logType='hdfs')
            self.inspect(data={'hdfsError': 'readHdfs ' + str(e)})

        runtime = time.time() - start
        output('read remote file completed used time %s' % runtime, logType='hdfs')
        return result

    def readLocal(self, filepath='', encoding='utf-8', callbackProcess=None, resultSet={}):

        start = time.time()
        output('read local file: %s start' % filepath, logType='hdfs')
        if isinstance(resultSet, type([])):
            result = []
            with open(filepath) as reader:
                for line in reader:
                    # 转换成字节码，加速运算
                    if isinstance(line, type(u'')):
                        line = line.decode('string-escape')
                    y = callbackProcess(x=line)
                    if y is not None:
                        result.append(y)
        else:
            result = resultSet
            with open(filepath) as reader:
                for line in reader:
                    # 转换成字节码，加速运算
                    if isinstance(line, type(u'')):
                        line = line.decode('string-escape')
                    if result is not None:
                        result = callbackProcess(x=line, y=result)

        runtime = time.time() - start
        output('read local file completed used time %s' % runtime, logType='hdfs')
        return result

    # 同一台机器，磁盘阵列决定并发写入效率
    @staticmethod
    def writeLocal(lines=[], filepath='', delimiter='\n'):
        start = time.time()
        tmp_file = PATH_CONFIG['tmp_path'] + filepath[len(PATH_CONFIG['hdfs_home']):]
        singleton.getinstance('pfile', 'core.libs.pfile').mkdirs(tmp_file, isFile=True)
        fp = open(tmp_file, 'a+')
        tmp_content = ''
        output('write local file: %s' % tmp_file, logType='hdfs')
        if lines and isinstance(lines, type([])):
            i = 0
            # 每10M写入一次
            each_num = 10*1024*1024/len(lines[0])
            for l in lines:
                i += 1
                tmp_content += l + delimiter
                if i >= each_num:
                    fp.write(tmp_content)
                    tmp_content = ''
                    i = 0
        elif lines:
            tmp_content = lines + delimiter
        if tmp_content:
            fp.write(tmp_content)
        fp.close()
        del tmp_content
        del lines

        output('write local file completed used time %s' % (time.time() - start), logType='hdfs')
        return tmp_file

    # 先写本地，再上传，避免连接数过多
    def uploadHdfs(self, remote_file='', local_file=''):
        start = time.time()
        output('upload local file: %s remote file: %s' % (local_file, remote_file), logType='hdfs')
        # 上传
        singleton.getinstance('phdfs', 'core.db.phdfs').mkdirs(os.path.dirname(remote_file))
        singleton.getinstance('phdfs', 'core.db.phdfs').client().upload(os.path.dirname(remote_file) + '/', local_file, overwrite=True)
        runtime = time.time() - start
        output('upload local file completed used time %s' % runtime, logType='hdfs')
        os.remove(local_file)
        return self

    # 下载
    def downloadHdfs(self, remote_file=''):
        local_file = PATH_CONFIG['tmp_path'] + remote_file[len(PATH_CONFIG['hdfs_home']):]
        start = time.time()
        output('download remote file: %s remote file: %s' % (local_file, remote_file), logType='hdfs')
        # 创建本地文件夹
        singleton.getinstance('pfile', 'core.libs.pfile').mkdirs(local_file, isFile=True)
        singleton.getinstance('phdfs', 'core.db.phdfs').client().download(remote_file, os.path.dirname(local_file) + '/', overwrite=True)
        runtime = time.time() - start
        output('download remote file completed used time %s' % runtime, logType='hdfs')
        return local_file

    def writeHdfs(self, lines=[], filepath='', overwrite=False, append=True, delimiter='\n', encoding='utf-8'):
        try:
            singleton.getinstance('phdfs', 'core.db.phdfs').client().status(filepath)
            singleton.getinstance('phdfs', 'core.db.phdfs').\
                write(data=lines, filepath=filepath, overwrite=overwrite, append=append, delimiter=delimiter, encoding=encoding)
        except Exception, e:
            if str(e).find('File does not exist') or str(e).find('not found') >= 0:
                # 不存在则创建文件
                singleton.getinstance('phdfs', 'core.db.phdfs').\
                    mkdirs(os.path.dirname(filepath)).\
                    write(data=lines, filepath=filepath, overwrite=True, append=False, delimiter='', encoding=encoding)
            else:
                self.inspect(data={'hdfsError': 'writeHdfs ' + str(e)})
                raise
        return self

    def mkdirsHdfs(self, filepath=''):
        singleton.getinstance('phdfs', 'core.db.phdfs').mkdirs(filepath=filepath)
        return self

    def deleteHdfs(self, filepath=''):
        singleton.getinstance('phdfs', 'core.db.phdfs').delete(filepath=filepath)
        return self

    def completedPrevHdfsTask(self):

        inspects = self.inspect()

        # 当前节点正在运行或已结束则退出
        if "mrcompleted" in inspects:
            return 'currrent_task_run'

        # mapper属于第一个任务，无需等待
        if self.taskName == 'mapper':
            return True

        # 上一层任务是否全部运行完成，完成则开始执行当前层级任务，否则退出
        completed_prev_task = True
        prev_ask = self.registerTask[self.registerTask.index(self.taskName)-1]
        usedNode = self.inspect(taskFullName=self.jobName + '.' + prev_ask, k='usedNode')
        if not usedNode:
            return False
        for sn in usedNode:
            thiscompleted = self.inspect(taskFullName=self.jobName + '.' + prev_ask, ipnode=sn, k='mrcompleted')
            if not thiscompleted:
                completed_prev_task = False
                break
        if not completed_prev_task:
            return False
        return True
