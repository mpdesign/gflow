# -*- coding: utf-8 -*-
# Filename: daemon.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-11-06
# Author:       mpdesign
# description:  守护脚本
# -----------------------------------
import sys
import os
import time
import atexit
import signal
from config.config import *


class Daemon:
    def __init__(self, pname=''):

        self.stdin = '/dev/stdin'
        self.stdout = '/dev/stdout'
        self.stderr = '/dev/stderr'
        self.pname = pname
        self.pidfile = "/tmp/slave_process_%s.pid" % pname
        self.logpath = "%s/%s/%s/%s" % (PATH_CONFIG['log_path'], time.strftime('%Y', time.localtime()), time.strftime('%m', time.localtime()), time.strftime('%d', time.localtime()))

    # 标准输出
    def set_stdfile(self, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        if stdin: stdin = self.logpath + '/' + stdin
        if stdout: stdout = self.logpath + '/' + stdout
        if stderr: stderr = self.logpath + '/' + stderr
        self.stdin = self.mkdirs(stdin)
        self.stdout = self.mkdirs(stdout)
        self.stderr = self.mkdirs(stderr)

    def _daemonize(self):

        try:
            pid = os.fork()
            if pid > 0:
                # 退出主进程
                sys.exit(0)
        except OSError, e:
            sys.stderr.write('fork #1 failed: %d (%s)' % (e.errno, e.strerror))
            sys.exit(1)

        os.chdir("/")
        os.setsid()
        os.umask(0)

        # 创建子进程
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write('fork #2 failed: %d (%s)' % (e.errno, e.strerror))
            sys.exit(1)

        # 重定向文件描述符
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        pid = int(os.getpid())
        # 判断pid是否已存在
        self.ifexistspid(pid, ifdel=True)
        # pid入栈
        self.pushpid(pid)

        # 程序退出时，从文件中删除当前进程pid
        atexit.register(self.delatexit, pid)

    # atexit退出清理相关操作
    def delatexit(self, pid):
        self.output('delatexit')
        self.ifexistspid(pid, ifdel=True)

    # 启动进程
    def start(self):

        self.output('Started LogFile Dir:' + self.logpath)
        self._daemonize()

    # 停止进程, whole 全部进程
    def stop(self, whole=False):
        if not self.pidfile or not os.path.isfile(self.pidfile):
            self.output('Daemon is not running')
            return False
        try:
            pf = open(self.pidfile, 'r')
            pids = pf.readlines()
            pf.close()
        except IOError:
            return False
        if not pids:
            return False

        if whole:
            for pid in pids:
                try:
                    os.kill(int(pid.strip()), signal.SIGTERM)
                except Exception,e:
                    continue
            os.remove(self.pidfile)
            self.output('Stopped %s process' % len(pids))
        else:
            # 杀死最近一个进程
            _pid = int(pids[len(pids)-1].strip())
            try:
                os.kill(_pid, signal.SIGTERM)
            except OSError, err:
                pass
            self.ifexistspid(_pid, ifdel=True)
            self.output('Stopped %s' % _pid)

    # 重启进程
    def restart(self):
        self.stop()
        self.start()

    # 创建多级目录
    @staticmethod
    def mkdirs(filename):
        if not os.path.isfile(filename):
            dirname = os.path.dirname(filename)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            f = open(filename, 'w')
            f.close()
            os.system("chmod -R 777 %s" % filename)

        return filename

    # pid是否存在文件中，ifdel：从文件中删除
    def ifexistspid(self, pid, ifdel=False):
        inpid = False
        if os.path.isfile(self.pidfile):
            pids = file(self.pidfile, 'r').readlines()
            # 空文件则删除
            if ifdel or not pids:
                os.remove(self.pidfile)

            if pids:
                for _pid in pids:
                    _pid = int(_pid.strip())
                    if pid ==_pid:
                        inpid = True
                        continue
                    if ifdel:
                        file(self.pidfile, 'a+').write('%s\n' % _pid)

        return inpid

    # pid入栈
    def pushpid(self, pid):
        file(self.pidfile, 'a+').write('%s\n' % pid)

    def output(self, msg=''):
        print "[%s @%s]" % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), self.pname), msg