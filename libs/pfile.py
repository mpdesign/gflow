# -*- coding: utf-8 -*-
import os
import fcntl
import time
import random
'''
锁变量
'''


# 文件 操作类
class pfile:

    # 初始化配置信息
    def __init__(self):
        self.f = ''
        self.fp = ''

    def set_file(self, filename='p.log'):
        self.f = filename
        self.mkdirs(filename, isFile=True)
        return self

    def write(self, content, method='a+', tocode='utf-8', fromcode='utf-8'):
        fp = open(self.f, method)
        while not self.lock(fp):
            time.sleep(0.1)

        if isinstance(content, unicode):
            content = content.encode(tocode)
        else:
            content = content.decode(fromcode).encode(tocode)

        #self.lock()
        fp.write(content)
        self.unlock(fp)

    # 读取一行内容
    def readline(self, method):
        fp = open(self.f, method)
        while not self.lock(fp):
            time.sleep(0.1)

        #self.lock()
        r = fp.readline()
        self.unlock(fp)
        return r

    # 读取所有行的内容
    def readlines(self, method):
        fp = open(self.f, method)
        while not self.lock(fp):
            time.sleep(0.1)

        #self.lock()
        r = fp.readlines()
        self.unlock(fp)
        return r

    #读取所有内容
    def read(self, method):
        fp = open(self.f, method)
        while not self.lock(fp):
            time.sleep(0.1)

        #self.lock()
        r = fp.read()
        self.unlock(fp)
        return r

    #创建目录
    @staticmethod
    def mkdir(dirname):
        os.mkdir(dirname)

    #删除目录
    @staticmethod
    def rmdir(dirname):
        os.rmdir(dirname)

    #创建多级目录
    @staticmethod
    def mkdirs(dirname, isFile=False):
        if isFile or os.path.isfile(dirname):
            dirname = os.path.dirname(dirname)
        if os.path.exists(dirname):
            return True
        os.makedirs(dirname)
        os.system("chmod -R 777 %s" % dirname)

    #文件大小
    @staticmethod
    def filesize(filename):
        return os.path.getsize(filename)

    #判断文件夹是否存在
    @staticmethod
    def path_exists(pathname):
        return os.path.exists(pathname)

    @staticmethod
    def isdir(pathname):
        return os.path.isdir(pathname)

    @staticmethod
    def isfile(filename):
        return os.path.isfile(filename)

    #文件锁
    @staticmethod
    def nb_lock(fp):
        try:
            fcntl.flock(fp, fcntl.LOCK_NB)
        except IOError, exc_value:
            #self.fp.close()
            #self.fp = None
            return False
        return True

    @staticmethod
    def lock(fp):
        fcntl.flock(fp, fcntl.LOCK_EX)
        return True

    @staticmethod
    def unlock(fp):
        fcntl.flock(fp, fcntl.LOCK_UN)
        fp.close()
        fp = None
        return True

    # 按个数均分或按大小或按行数分割文件
    def split(self, filename='', num=0, size=0, line=0):
        if not filename or not os.path.exists(filename):
            print '%s is not exists' % filename
        try:
            if line > 0:
                self.splitByLine(filename=filename, line=line)
            elif num > 1:
                self.splitByNum(filename=filename, num=num)
            elif size > 0:
                self.splitBySize(filename=filename, size=size)
            else:
                print '%s num|size|line is not validate' % filename
                return [filename]
            return self.part_file_name.values()
        except IOError as err:
            print(err)
        else:
            print("%s is not a validate file" % filename)
        return None

    def splitBySize(self, filename='', size=0):
        with open(filename) as f:
            temp_size = 0
            temp_content = []
            part_num = 0
            for l in f:
                if temp_size < size:
                    temp_size += len(l)
                else:
                    self.splitWriteFile(filename, part_num, temp_content)
                    part_num += 1
                    temp_size = len(l)
                    temp_content = []
                temp_content.append(l)
            else:
                # 正常结束循环后将剩余的内容写入新文件中
                self.splitWriteFile(filename, part_num, temp_content)

    def splitByNum(self, filename='', num=0):
        filesize = os.path.getsize(filename)
        size = int(filesize/num)
        self.splitBySize(filename=filename, size=size)

    def splitByLine(self, filename='', line=0):
        with open(filename) as f:
            temp_count = 0
            temp_content = []
            part_num = 0
            for l in f:
                if temp_count < line:
                    temp_count += 1
                else:
                    self.splitWriteFile(filename, part_num, temp_content)
                    part_num += 1
                    temp_count = 1
                    temp_content = []
                temp_content.append(l)
            else:
                # 正常结束循环后将剩余的内容写入新文件中
                self.splitWriteFile(filename, part_num, temp_content)

    def splitWriteFile(self, filename, part_num, *line_content):
        """将按行分割后的内容写入相应的分割文件中"""
        part_file_name = self.splitPartFileName(filename, part_num)
        # print(line_content)
        try:
            with open(part_file_name, "w") as part_file:
                part_file.writelines(line_content[0])
        except IOError as err:
            print(err)

    def splitPartFileName(self, filename, part_num):
        """"
        获取分割后的文件名称：在源文件相同目录下建立临时文件夹temp_part_file，然后将分割后的文件放到该路径下
        """
        file_key = "%s_%s" % (filename, part_num)
        if not hasattr(self, 'part_file_name'):
            setattr(self, "part_file_name", {})
        if file_key in self.part_file_name.keys():
            return self.part_file_name[file_key]
        if os.path.isdir(filename):
            dirname = filename.rstrip('/')
            part_file_path = dirname + "/tmp.part"
            i = "%s%s" % (time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())), random.randint(1000, 9999))
            part_file_name = "temp" + i + "." + str(part_num) + ".part"
        else:
            basename = os.path.basename(filename)
            dirname = os.path.dirname(filename)
            part_file_path = dirname + "/tmp.part"
            part_file_name = basename + '.' + str(part_num) + ".part"
        # 如果临时目录不存在则创建
        if not os.path.isdir(part_file_path):
            self.mkdirs(part_file_path)
        part_file_name = part_file_path + "/" + part_file_name
        self.part_file_name[file_key] = part_file_name
        return part_file_name
