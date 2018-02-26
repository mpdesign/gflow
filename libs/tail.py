# -*- coding: utf-8 -*-
# hadoop 必须先创建文件夹，并可写 /gflow/logs/

import os
import sys
import time
import re
from core.comm.singleton import *
from core.worker import *


class tail(object):
    
    def __init__(self):
        self.callback_data=None
        # 本次启动采集的文件随机数
        self.file_rand_id = random.randint(1, 10000)
        self.config = {}
        self.conf()

    def conf(self,
             # 每次采集字节数
             chunk_size=1,
             # 管道队列长度
             buffer_size=1000000,
             # 每达到一百行写入sink
             batch_size=100,
             # 超过10秒没有tail到数据，写入sink
             batch_interval=10,
             # 日志采集源目录
             sourceFile='/logs/nginx',
             # 写入目录
             sinkFolder='/gflow/logs',
             # 临时目录
             tmpFolder=PATH_CONFIG['tmp_path'] + '/tail',
             encoding='utf8',
             # sink to hdfs定时策略[每小时5分, 每天凌晨1点]
             atSink=['H5', 'd1'],
             channelPoolLen=1,
             sinkPoolLen=1,
             uploadPoolLen=1,
             # 是否压缩后sink
             compress=False):
        self.config = {
            'chunk_size': chunk_size,
            'buffer_size': buffer_size,
            'batch_interval': batch_interval,
            'batch_size': batch_size,
            # 文件夹：/name/Y/m/d/
            'sourceFile': sourceFile.rstrip('/'),
            'sinkFolder': sinkFolder.rstrip('/'),
            'tmpFolder': tmpFolder.rstrip('/'),
            'encoding': encoding,
            'compress': compress,
            'atSink': atSink,
            'channelPoolLen': channelPoolLen,
            'sinkPoolLen': sinkPoolLen,
            'uploadPoolLen': uploadPoolLen
        }
        return self

    def register_callback(self, callback='', data=None):
        self.callback_data = data
        tail.callback = staticmethod(callback)
        return self

    def check_file_validity(self):
        """ Check whether the a given file exists, readable and is a file """
        tail_files = []

        if singleton.getinstance('pfile').isfile(self.config['sourceFile']) or self.config['sourceFile'].split('/')[-1].find('*'):
            folder = os.path.dirname(self.config['sourceFile'])
            pattern = os.path.basename(self.config['sourceFile'])
        else:
            folder = self.config['sourceFile']
            pattern = '*'

        if not os.access(self.config['sourceFile'], os.R_OK):
            raise TailError("File '%s' not readable" % self.config['sourceFile'])

        for root, dirs, files in os.walk(folder):
            if files:
                for f in files:
                    # 匹配对应的文件
                    if not re.search(pattern, f):
                        continue
                    # 检查最近一次偏移位置 position
                    tail_file = root.rstrip('/') + '/' + f
                    # 偏移位置文件
                    pfile = self.getPosFilePath(tail_file=tail_file)
                    # 不存在则创建
                    if not singleton.getinstance('pfile').isfile(pfile):
                        singleton.getinstance('pfile').mkdirs(pfile, True)
                        tail_pos = 0
                    else:
                        fp = open(pfile, 'r')
                        tail_pos = fp.read()
                        fp.close()
                    tail_files.append((tail_file, tail_pos))
        if not tail_files:
            raise TailError('sourceFile %s has not any file' % self.config['sourceFile'])

        return tail_files

    # 多线程监听文件夹下的所有文件
    def follow(self):
        # 检查文件
        tail_files = self.check_file_validity()

        # queue
        channelQueue = Queue.Queue()
        channelQueue.maxsize = self.config['buffer_size']
        sinkQueue = Queue.Queue()
        sinkQueue.maxsize = self.config['buffer_size']

        # source
        wnum = len(tail_files)
        sourcePool = WorkerManager(wnum)
        sourcePool.parallel_for_complete()
        for tail_file, tail_pos in tail_files:
            sourcePool.add(self.source, tail_file=tail_file, tail_pos=tail_pos, outputQueue=channelQueue)
        sourcePool.stop(afterwork=False)

        # channel
        channelPool = WorkerManager(self.config['channelPoolLen'])
        channelPool.parallel_for_complete()
        channelPool.add(self.channel, inputQueue=channelQueue, outputQueue=sinkQueue)
        channelPool.stop(afterwork=False)

        # sink
        sinkPool = WorkerManager(self.config['sinkPoolLen'])
        sinkPool.parallel_for_complete()
        sinkPool.add(self.sink, inputQueue=sinkQueue)
        sinkPool.stop(afterwork=False)

        # hdfs
        hdfsPool = WorkerManager(2)
        hdfsPool.parallel_for_complete()
        # 每小时上传至hdfs
        hdfsPool.add(self.sinkHourFile)
        # 每天合并压缩上传至hdfs
        hdfsPool.add(self.mergeFileHour2Day)
        hdfsPool.stop(afterwork=False)

        while hdfsPool.aliveWorkers():
            time.sleep(3600)

        raise TailError('Task tail.dc execution has gone out of time')

    def source(self, tail_file='', tail_pos=0, outputQueue=None):
        with open(tail_file) as file_:
            # Go to the end of file
            # 从末尾开始遍历
            if tail_pos == 'end':
                file_.seek(0, 2)
                tail_pos = file_.tell()
            else:
                tail_pos = intval(tail_pos)
                file_.seek(tail_pos, 0)
            while True:
                for line in file_.readlines(self.config['chunk_size']):
                    tail_pos = file_.tell()
                    try:
                        outputQueue.put_nowait((tail_file, tail_pos, line))
                    except Exception, e:
                        self.setPosition(tail_file=tail_file, tail_pos=tail_pos)
                        raise TailError('channelQueue.put_nowait %s' % str(e))
                else:
                    file_.seek(tail_pos, 0)
                    # 记录最新偏移位置
                    self.setPosition(tail_file=tail_file, tail_pos=tail_pos)
                    time.sleep(3)

    def channel(self, inputQueue=None, outputQueue=None):
        while True:
            try:
                data = inputQueue.get(timeout=None)
            except Exception, e:
                raise TailError('channel.inputQueue get error %s' % str(e))

            tail_file, tail_pos, line = data
            res = tail.callback(line, self.callback_data)
            if not res:
                continue
            app_id, topic_name, data_time, log_time, row = res
            ymdh = time.strftime('%Y%m%d%H', time.localtime(data_time))
            # 按app_id、主题、数据时间、日志时间、文件随机数分割文件
            topic_file = '/%s/%s/h-%s-%s.%s.log' % (app_id, topic_name, ymdh, time.strftime('%Y%m%d%H', time.localtime(log_time)), self.file_rand_id)
            try:
                outputQueue.put_nowait((tail_file, tail_pos, row, topic_file))
            except Exception, e:
                self.setPosition(tail_file=tail_file, tail_pos=tail_pos)
                raise TailError('sinkQueue.put_nowait %s' % str(e))

    def sink(self, inputQueue=None):
        sinkto = False
        batch_timeout = self.config['batch_interval']
        batch = {}
        batch_pos = {}
        while True:

            try:
                data = inputQueue.get(timeout=batch_timeout)
            except Exception, e:
                data = 'timeout'

            # 若无数据，则落地
            if isinstance(data, type('')) and data == 'timeout':
                batch_timeout += self.config['batch_interval']
                if batch_timeout > 60:
                    batch_timeout += 50
                    time.sleep(60)
                sinkto = True
            else:
                tail_file, tail_pos, line, topic_file = data
                if topic_file not in batch:
                    batch[topic_file] = {'lines': '', 'tail_file': {}, 'rows': 0}
                batch_pos[tail_file] = tail_pos
                batch[topic_file]['lines'] += line
                batch[topic_file]['rows'] += 1
                if batch[topic_file]['rows'] >= self.config['batch_size']:
                    sinkto = topic_file

            if sinkto:
                del_topic_file = {}
                try:
                    for topic_file in batch:
                        filepath = '%s%s' % (self.config['sinkFolder'], topic_file)
                        # 先落地至本地缓存
                        self.writeLocal(lines=batch[topic_file]['lines'], filepath=filepath)
                        del_topic_file[topic_file] = 1
                        # 异步上传至hdfs
                except Exception, e:
                    # 保存最后的偏移位置
                    for tail_file in batch_pos:
                        self.setPosition(tail_file=tail_file, tail_pos=batch_pos[tail_file])
                    raise TailError('sink.writeLocal %s' % str(e))

                batch_timeout = 0
                sinkto = False
                # 注销
                for dtf in del_topic_file:
                    del batch[dtf]

    def writeLocal(self, lines=[], filepath=''):
        tmp_file = '%s%s' % (self.config['tmpFolder'], filepath)
        singleton.getinstance('pfile', 'core.libs.pfile').mkdirs(tmp_file, isFile=True)
        fp = open(tmp_file, 'a+')
        fp.write(lines)
        fp.close()
        return True

    def sinkHourFile(self):
        now = True if 'now' in argv_cli['dicts'] else False
        # 每小时上传一次上一小时的文件, 并删除本地缓存
        while True:
            if not now and int(time.strftime('%M', time.localtime())) != int(self.config['atSink'][0][1:]):
                time.sleep(50)
                continue
            now = False

            localhdfspath = '%s%s' % (self.config['tmpFolder'], self.config['sinkFolder'])
            if not singleton.getinstance('pfile').isdir(localhdfspath):
                continue

            # 上传线程池
            uploadPool = WorkerManager(self.config['uploadPoolLen'])
            uploadPool.parallel_for_complete()
            start_time = time.time()
            prev_hour = int(time.strftime('%Y%m%d%H', time.localtime(time.time()-3600)))
            for root, dirs, files in os.walk(localhdfspath):
                # 合并小时文件
                if not files:
                    continue
                hfiles = {}
                for f in files:
                    if f[0:2] != 'h-':
                        continue
                    _h = f[2:12]
                    if _h not in hfiles:
                        hfiles[_h] = []
                    hfiles[_h].append(f)

                for _h in hfiles:
                    # 时间未到
                    if intval(_h) > prev_hour:
                        continue
                    # 已处理过
                    completed_file = root + '/h' + _h + '.completed'
                    if singleton.getinstance('pfile').isfile(completed_file):
                        continue
                    uploadPool.add(self._mergerFileHour2Hour, root, _h, hfiles[_h], completed_file)

            # 队列完成则退出
            uploadPool.stop(afterwork=False)
            while uploadPool and uploadPool.aliveWorkers():
                time.sleep(10)
            output('sinkHourFile used time %s' % (time.time() - start_time), logType='tail')
            del uploadPool

    # 合并小时文件，压缩并上传
    def _mergerFileHour2Hour(self, root, _h, _hfiles, completed_file):
        # 新增合并文件
        new_hour_file = '%s/h-%s-%s.%s.log' % (root, _h, time.strftime('%Y%m%d', time.localtime()), self.file_rand_id)
        # 合并本地小时文件
        os.system('cat %s/h-%s-*.log > %s' % (root, _h, new_hour_file))

        # 删除小文件, 保留合并后的大文件，供隔天合并天文件
        for f in _hfiles:
            os.remove(root + '/' + f)

        # 压缩合并后的文件
        remote_file = new_hour_file.replace(self.config['tmpFolder'], '')
        start = time.time()
        if self.config['compress']:
            upload_file = new_hour_file + '.gz'
            os.system('gzip -c %s > %s' % (new_hour_file, upload_file))
            output('compress local file: %s used time %s' % (upload_file, time.time() - start), logType='tail')
        else:
            upload_file = new_hour_file

        # 上传合并后的文件
        start = time.time()
        singleton.getinstance('phdfs', 'core.db.phdfs').mkdirs(os.path.dirname(remote_file))
        singleton.getinstance('phdfs', 'core.db.phdfs').upload(os.path.dirname(remote_file) + '/', upload_file, overwrite=True)
        runtime = time.time() - start
        output('upload local file completed: %s used time %s' % (upload_file, runtime), logType='tail')

        # 标识该小时的文件已上传
        fp = open(completed_file, 'a')
        fp.write('')
        fp.close()

        # 上传后，删除本地压缩文件 .gz
        if self.config['compress']:
            os.remove(upload_file)

    # 合并本地文件
    def mergeFileHour2Day(self):
        # 凌晨1点执行今天之前的所有数据
        now = True if 'now' in argv_cli['dicts'] else False
        while True:
            if not now and int(time.strftime('%H', time.localtime())) != int(self.config['atSink'][1][1:]):
                time.sleep(1800)
                continue
            now = False
            localhdfspath = '%s%s' % (self.config['tmpFolder'], self.config['sinkFolder'])
            yday = time.strftime('%Y%m%d', time.localtime(time.time()-24*3600))
            if not singleton.getinstance('pfile').isdir(localhdfspath):
                continue

            for root, dirs, files in os.walk(localhdfspath):
                # 合并小时文件
                if not files:
                    continue
                dfiles = {}
                for f in files:
                    if f[0:2] != 'h-':
                        # 未上传的天文件
                        day = intval(f[0:8])
                    else:
                        day = intval(f[2:10])
                    # 只处理今天之前的所有小时文件
                    if day < 19710000 or day > int(yday):
                        continue
                    if day not in dfiles:
                        dfiles[day] = []
                    dfiles[day].append(f)

                for day in dfiles:
                    day_folder = root
                    # singleton.getinstance('pfile').mkdirs(day_folder)
                    hour_folder = root
                    # 新增随机数，识别不同文件
                    new_day_file = '%s/%s.%s.log' % (day_folder, time.strftime('%Y%m%d', time.localtime()), self.file_rand_id)
                    # 合并本地小时文件
                    for f in dfiles[day]:
                        os.system('cat %s/%s >> %s' % (hour_folder, f, new_day_file))

                    # 压缩 .log.gz
                    if self.config['compress']:
                        upload_file = new_day_file + '.gz'
                        os.system('gzip -c %s > %s' % (new_day_file, upload_file))
                    else:
                        upload_file = new_day_file

                    # 上传至线上
                    remote_day_folder = day_folder.replace(self.config['tmpFolder'], '')
                    start = time.time()
                    singleton.getinstance('phdfs', 'core.db.phdfs').mkdirs(remote_day_folder)
                    singleton.getinstance('phdfs', 'core.db.phdfs').upload(remote_day_folder + '/', upload_file, overwrite=True)
                    output('upload local file: %s completed used time %s' % (upload_file, time.time() - start), logType='tail')

                    # 删除线上小时文件 /h/*
                    remote_hour_folder = hour_folder.replace(self.config['tmpFolder'], '')
                    singleton.getinstance('phdfs', 'core.db.phdfs').delete(remote_hour_folder + '/h-%s*.log' % day)

                    # 删除本地小时文件 h-{hour}*.log
                    os.system('rm -rf %s/h-%s*.log' % (hour_folder, day))
                    # 删除本地小时完成标识文件 h{hour}.completed
                    os.system('rm -rf %s/h%s*.completed*' % (hour_folder, day))
                    # 删除本地天文件 *.log  *.log.gz
                    os.system('rm -rf %s/%s*' % (day_folder, day))

    # 偏移量存储文件
    def getPosFilePath(self, tail_file=''):
        return '%s/position/%s.pos' % (self.config['tmpFolder'], os.path.basename(tail_file)[0:12] + '-' + md5(tail_file)[0:6])

    # 设置偏移位置
    def setPosition(self, tail_file='', tail_pos=0):
        posf = self.getPosFilePath(tail_file)
        if not singleton.getinstance('pfile').isfile(posf):
            singleton.getinstance('pfile').mkdirs(posf, True)

        fp = open(posf, 'w')
        fp.write(str(tail_pos))
        fp.close()


class TailError(Exception):

    def __init__(self, msg):
        self.message = msg
        output(msg, taskName='tail', logType='run')

    def __str__(self):
        return self.message
