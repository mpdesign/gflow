# -*- coding: utf-8 -*-

import hdfs
from core.comm.common import *
import requests
rs = requests.session()


class phdfs:

    def __init__(self):
        self._client = None
        self.filepath = ''
        self.delimiter = '\n'
        self.encoding = 'utf8'

    # 连接
    def client(self, host='', port='', timeout=None):
        host = host or HDFS_CONFIG['host']
        port = port or HDFS_CONFIG['webport']
        if self._client is None:
            try:
                self._client = hdfs.Client('http://%s:%s' % (host, port), timeout=timeout, session=rs)
            except Exception, e:
                output('hdfs Exception ' + str(e), logType='hdfs')
                raise
        return self._client

    # 设置文件
    def setfile(self, filepath='', delimiter='\n', encoding='utf8'):
        self.client()
        self.filepath = filepath
        self.delimiter = delimiter
        self.encoding = encoding
        return self

    # 文件分块列表，截止delimiter，hdfs默认128M 按行取整；可自定义块数block_num
    def blockList(self, block_num=0, blcok_size=0):
        fileinfo = self.client().status(self.filepath)
        file_size = fileinfo['length']

        if block_num > 0:
            block_size = intval(file_size/block_num)
        elif blcok_size < 1:
            block_size = fileinfo['blockSize']
        size_list = []
        offset = 0
        findlength = block_size
        find_file_last_incomplete_line_times = 0
        while True:
            # 找出最后没有分隔符的行, 确定块的分割位置
            length, incomplete_size = self.findLastIncompleteLine(offset=offset, length=findlength)
            if length > 0:
                size_list.append({"offset": offset, "length": length, "filepath": self.filepath})
            # 分割完毕
            if offset + findlength >= file_size:
                # 找不到文件的最后一行分隔符，次数达到两次则结束
                find_file_last_incomplete_line_times += 1
                if find_file_last_incomplete_line_times > 1:
                    size_list.append({"offset": offset, "length": incomplete_size, "filepath": self.filepath})
                    break
                if incomplete_size == 0:
                    break
            offset += length
            findlength = block_size + incomplete_size
        return size_list

    # 分割读取
    def readlines(self, offset=0, length=None, buffer_size=65536):
        start = time.time()
        result = []
        output('read file: %s?offset[%s],length[%s] start' % (self.filepath, offset, length), logType='hdfs')
        try:
            with self.client().read(self.filepath, offset=offset, length=length,
                                    buffer_size=buffer_size,
                                    delimiter=self.delimiter,
                                    encoding=self.encoding) as reader:
                # 注意：必须加上 delimiter='\n', encoding='utf8'，否则需要加上r.read()，不然读取速度会很慢
                for line in reader:
                    result.append(line)
        except Exception, e:
            output(('readlines', e), logType='hdfs')

        runtime = time.time() - start
        output('read file completed used time %s' % runtime, logType='hdfs')
        return result

    # 一次读取
    def read(self, filepath='', offset=0, length=None, encoding='utf-8', max_retries=5):
        filepath = filepath or self.filepath
        encoding = encoding or self.encoding
        lines = ''
        # 尝试连接
        i = 0
        while i < max_retries:
            try:
                lines = self._read(filepath=filepath, offset=offset, length=length, encoding=encoding)
                break
            except Exception, e:
                if str(e).find('connection') >= 0:
                    time.sleep(random.randint(1, 10))
                    i += 1
                    output(('phdfs.read has retried %s, max retries[%s]' % (i, max_retries)) + str(e), logType='hdfs')
                    continue
                else:
                    # 抛出异常，供外部使用
                    raise e, None, sys.exc_info()[2]
        return lines

    def _read(self, filepath='', offset=0, length=None, encoding='utf-8'):
        filepath = filepath or self.filepath
        encoding = encoding or self.encoding
        with self.client().read(filepath, offset=offset, length=length,
                                        encoding=encoding) as reader:
                return reader.read()

    # 写入, 不存在则强制抛出异常，供外部使用
    def write(self, data=None, overwrite=False, append=True, filepath='', delimiter='\n', encoding='utf-8'):
        filepath = filepath or self.filepath
        delimiter = delimiter or self.delimiter
        encoding = encoding or self.encoding
        start = time.time()
        if filepath[-4:] != '.ins':
            output('write file: %s start' % filepath, logType='hdfs')
        if isinstance(data, type([])):
            lines = ''
            if data:
                for d in data:
                    lines += d + delimiter
        else:
            lines = data + delimiter
        self.client().write(filepath, data=lines, overwrite=overwrite, append=append, encoding=encoding)
        runtime = time.time() - start
        if filepath[-4:] != '.ins':
            output('write file completed, used time %s' % runtime, logType='hdfs')
        return self

    # 动态创建文件夹
    def mkdirs(self, filepath=''):
        self.client().makedirs(filepath)
        return self

    # 删除文件或文件夹
    def delete(self, filepath='', recursive=True):
        self.client().delete(filepath, recursive=recursive)
        return self

    # 查找最后不完整的行
    def findLastIncompleteLine(self, offset=0, length=0):
        incomplete_size = 0
        chunk_size = min(1024, length)
        ost = offset + length
        k = 1
        while ost >= offset:
            ost = offset + length - chunk_size*k
            try:
                chunk_conyent = self.read(offset=ost, length=chunk_size)
            except Exception, e:
                output(('phdfs.findLastIncompleteLine.read() offset[%s],length[%s],ost[%s],chunk_size[%s]' % (offset, length, ost, chunk_size), e), logType='hdfs')
                if str(e).find("out of the range"):
                    break
                raise
            if not chunk_conyent:
                break
            chunk_list = chunk_conyent.split(self.delimiter)
            # 未找到全部
            if len(chunk_list) == 1:
                # 继续找
                continue
            # 找到
            elif len(chunk_list) > 1:
                if chunk_list[-1] == '':
                    # 刚好截止，则退出
                    incomplete_size = chunk_size*(k-1)
                else:
                    incomplete_size = chunk_size*(k-1) + len(chunk_list[-1])
                break
        # 剪掉当前块最后没有分隔符的行
        length -= incomplete_size
        return length, incomplete_size