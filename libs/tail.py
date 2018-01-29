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

        self.callback = sys.stdout.write
        self.tmpFolder = PATH_CONFIG['tmp_path'] + '/tail'
        self.config = {}
        self.conf()
        self.atSink = ['H5', 'd1']

    def conf(self, buffer_size=100000, batch_size=100, batch_interval=10, sourceFolder='/logs/nginx', sinkFolder='/gflow/logs', encoding='utf8', compress=False):
        # 每一百行回写到hdfs
        self.config = {
            'buffer_size': buffer_size,
            'batch_interval': batch_interval,
            'batch_size': batch_size,
            # 文件夹：/name/Y/m/d/
            'sourceFolder': sourceFolder.rstrip('/'),
            'sinkFolder': sinkFolder.rstrip('/'),
            'encoding': encoding,
            'compress': compress
        }
        return self

    def check_file_validity(self):
        """ Check whether the a given file exists, readable and is a file """
        tail_files = []

        if singleton.getinstance('pfile').isfile(self.config['sourceFolder']) or self.config['sourceFolder'].split('/')[-1].find('*'):
            folder = os.path.dirname(self.config['sourceFolder'])
            pattern = os.path.basename(self.config['sourceFolder'])
        else:
            folder = self.config['sourceFolder']
            pattern = '*'

        if not os.access(self.config['sourceFolder'], os.R_OK):
            raise TailError("File '%s' not readable" % self.config['sourceFolder'])

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
            raise TailError('sourceFolder %s has not any file' % self.config['sourceFolder'])

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
        channelPool = WorkerManager(1)
        channelPool.parallel_for_complete()
        channelPool.add(self.channel, inputQueue=channelQueue, outputQueue=sinkQueue)
        channelPool.stop(afterwork=False)

        # sink
        sinkPool = WorkerManager(1)
        sinkPool.parallel_for_complete()
        sinkPool.add(self.sink, inputQueue=sinkQueue)
        sinkPool.stop(afterwork=False)

        # hdfs
        hdfsPool = WorkerManager(2)
        hdfsPool.parallel_for_complete()
        # 每小时上传至hdfs
        hdfsPool.add(self.sinkHourFile)
        # 每天合并压缩上传至hdfs
        hdfsPool.add(self.mergeHourFile)
        hdfsPool.stop(afterwork=False)

        while hdfsPool.aliveWorkers():
            time.sleep(3600)

        raise TailError('Task %s execution has gone out of time' % self.config['sourceFolder'])

    def source(self, tail_file='', tail_pos=0, outputQueue=None):
        setted = False
        with open(tail_file) as file_:
            # Go to the end of file
            # 从末尾开始遍历
            if tail_pos == 'end':
                file_.seek(0, 2)
            else:
                file_.seek(intval(tail_pos), 0)
            while True:
                line = file_.readline()
                tail_pos = file_.tell()
                print 'line', line, tail_pos
                if not line:
                    file_.seek(tail_pos, 0)
                    # 记录最新偏移位置
                    if not setted:
                        self.setPosition(tail_file=tail_file, tail_pos=tail_pos)
                        setted = True
                    time.sleep(3)
                else:
                    try:
                        outputQueue.put_nowait((tail_file, tail_pos, line))
                        setted = False
                    except Exception, e:
                        self.setPosition(tail_file=tail_file, tail_pos=tail_pos)
                        raise TailError('channelQueue.put_nowait %s' % str(e))

    def channel(self, inputQueue=None, outputQueue=None):
        while True:
            try:
                data = inputQueue.get(timeout=None)
            except Exception, e:
                raise TailError('channel.inputQueue get error %s' % str(e))

            if isinstance(data, type('')) and data == 'timeout':
                continue
                # _exit(1)

            tail_file, tail_pos, line = data
            line = line.rstrip()
            m = re.search(r'/da/v[\d\.]+/([\w\d]+)[\?|\s]+(.*?)&time=([\d]+)', line)
            if m:
                topic_name = m.group(1)
                _time = floatval(m.group(3))
                if _time > 1000000000:
                    # 按主题、天、数据产生小时分割文件
                    topic_file = '/%s/%s/h/%s' % (topic_name, time.strftime('%Y%m%d', time.localtime(_time)), time.strftime('%Y%m%d%H', time.localtime(_time)))
                    try:
                        outputQueue.put_nowait((tail_file, tail_pos, line, topic_file))
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
                batch[topic_file]['lines'] += line + '\n'
                batch[topic_file]['rows'] += 1
                if batch[topic_file]['rows'] >= self.config['batch_size']:
                    sinkto = topic_file

            if sinkto:
                del_topic_file = {}
                try:
                    for topic_file in batch:
                        # 新增随机数，识别不同文件
                        filepath = '%s%s-%s.%s.log' % (self.config['sinkFolder'], topic_file, time.time(), random.randint(1, 1000))
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
        tmp_file = '%s%s' % (self.tmpFolder, filepath)
        singleton.getinstance('pfile', 'core.libs.pfile').mkdirs(tmp_file, isFile=True)
        fp = open(tmp_file, 'a+')
        fp.write(lines)
        fp.close()
        return True

    def sinkHourFile(self):
        # 每小时上传一次上一小时的文件, 并删除本地缓存
        while True:
            if int(time.strftime('%M', time.localtime())) != self.atSink[0][1:]:
                time.sleep(50)
                continue
            prev_hour = int(time.strftime('%Y%m%d%H', time.localtime(time.time()-3600)))
            localhdfspath = '%s%s' % (self.tmpFolder, self.config['sinkFolder'])
            for root, dirs, files in os.walk(localhdfspath):
                # 不存在文件夹和文件，则删除该目录
                if not dirs and not files:
                    os.removedirs(root)
                    continue
                if files:
                    for f in files:
                        # 压缩文件不再压缩
                        if f[-3:] == '.gz':
                            continue
                        # 存在对应的压缩文件不处理
                        if singleton.getinstance('pfile').isfile(f + '.gz'):
                            continue
                        if f.find('/d/') > 0:
                            continue
                        local_file = root.rstrip('/') + '/' + f
                        hour_name = os.path.basename(local_file)[0:10]
                        # 只上传上一个小时之前的文件
                        if intval(hour_name) < 20000000 or intval(hour_name) > prev_hour:
                            continue
                        remote_file = local_file.replace(self.tmpFolder, '')
                        start = time.time()
                        local_file_gz = local_file + '.gz'
                        os.system('gzip -c %s > %s' % (local_file, local_file_gz))
                        output('compress local file: %s used time %s' % (local_file_gz, time.time() - start), logType='hdfs')
                        # 上传
                        start = time.time()
                        singleton.getinstance('phdfs', 'core.db.phdfs').mkdirs(os.path.dirname(remote_file))
                        singleton.getinstance('phdfs', 'core.db.phdfs').client().upload(os.path.dirname(remote_file) + '/', local_file_gz, overwrite=True)
                        runtime = time.time() - start
                        output('upload local file completed used time %s' % runtime, logType='hdfs')
            time.sleep(60)

    # 合并本地文件
    def mergeHourFile(self):
        # 凌晨1点执行今天之前的所有数据
        while True:
            if int(time.strftime('%H', time.localtime())) != self.atSink[1][1:]:
                time.sleep(1800)
                continue
            localhdfspath = '%s%s' % (self.tmpFolder, self.config['sinkFolder'])
            yday = time.strftime('%Y%m%d', time.localtime(time.time()-24*3600))
            # 列出所有文件夹  /ymd
            if singleton.getinstance('pfile').isdir(localhdfspath):
                for topic_name in os.listdir(localhdfspath):
                    for day in os.listdir(localhdfspath + '/' + topic_name):
                        if day > yday:
                            continue
                        # /ymd/*
                        topic_folder = localhdfspath + '/' + topic_name + '/' + day
                        day_folder = topic_folder + '/d'
                        hour_folder = topic_folder + '/h'
                        # 新增随机数，识别不同文件
                        new_day_file = '%s/%s-%s.%s.log' % (day_folder, day, time.time(), random.randint(1, 1000))
                        # 合并本地小时文件/h/*.log =>/d/.log
                        os.system('cat %s/*.log > %s' % (hour_folder, new_day_file))
                        # 压缩 /d/.log => /d/.log.gz
                        gz_file = new_day_file + '.gz'
                        os.system('gzip -c %s > %s' % (new_day_file, gz_file))
                        # 上传至线上，天的文件夹
                        remote_day_folder = day_folder.replace(self.tmpFolder, '')
                        start = time.time()
                        singleton.getinstance('phdfs', 'core.db.phdfs').mkdirs(remote_day_folder)
                        singleton.getinstance('phdfs', 'core.db.phdfs').client().upload(remote_day_folder + '/', gz_file, overwrite=True)
                        output('upload local file: %s completed used time %s' % (gz_file, time.time() - start), logType='hdfs')
                        # 删除线上小时文件 /h/*
                        remote_hour_folder = hour_folder.replace(self.tmpFolder, '')
                        singleton.getinstance('phdfs', 'core.db.phdfs').delete(remote_hour_folder)
                        # 删除本地文件
                        os.system('rm -rf %s/*' % topic_folder)
            time.sleep(3600)

    # 偏移量存储文件
    def getPosFilePath(self, tail_file=''):
        return '%s/position/%s.pos' % (self.tmpFolder, os.path.basename(tail_file)[0:12] + '-' + md5(tail_file)[0:6])

    # 设置偏移位置
    def setPosition(self, tail_file='', tail_pos=0):
        fp = open(self.getPosFilePath(tail_file), 'r+')
        fp.write(str(tail_pos))
        fp.close()


class TailError(Exception):

    def __init__(self, msg):
        self.message = msg
        output(msg, taskName='tail', logType='run')

    def __str__(self):
        return self.message