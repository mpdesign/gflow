# -*- coding: utf-8 -*-
# Filename: master

# -----------------------------------
# Revision:     2.0
# Date:         2017-11-06
# Author:       mpdesign
# description:  批量操作服务器（执行命令，上传，下载）
# -----------------------------------

import paramiko
from common.common import *


class ptelnet:

    def __init__(self):
        self.logfile = '%s/%s/telnet.log' % (PATH_CONFIG["log_path"], time.strftime('%Y/%m/%d', time.localtime()))
        self._timeout = None
        pass

    def timeout(self, timeout=None):
        self._timeout = timeout
        return self

    def ssh2(self, **kwargs):
        if not kwargs["host"]:
            output('host is null', logType='telnet')
            return
        if not kwargs["action"]:
            output('action is null', logType='telnet')
            return
        sshPool = WorkerManager(len(kwargs["host"]))
        sshPool.parallel_for_complete().stop()
        for h in kwargs["host"]:
            ip2 = h["ip"]
            ssh_info = db().query("select * from %s where db='slave' and host='%s' limit 1" % (CONFIG_TABLE, ip2))
            if not ssh_info or not isinstance(ssh_info, type({})):
                output('%s has not been config in db' % ip2, logType='telnet')
                continue
            user = ssh_info['user']
            passwd = ssh_info['password']
            if not ip2 or not user:
                output('ip or user or passwd is null', logType='telnet')
                return

            if kwargs["action"] == 'ssh':
                if not kwargs["command"]:
                    output('command is null', logType='telnet')
                    return
                sshPool.add(self.ssh, ip2, user, passwd, kwargs["command"])
                # t = threading.Thread(target=self.ssh, args=(ip2, user, passwd, kwargs["command"]))
            else:
                if not kwargs["local_dir"] or not kwargs["remote_dir"]:
                    output('command is null', logType='telnet')
                    return
                local_dir = kwargs["local_dir"]
                remote_dir = kwargs["remote_dir"]
                if kwargs["action"] == 'download':
                    sshPool.add(self.download, ip2, user, passwd, local_dir, remote_dir)
                    # t = threading.Thread(target=self.download, args=(ip2, user, passwd, local_dir, remote_dir))
                elif kwargs["action"] == 'upload':
                    sshPool.add(self.upload, ip2, user, passwd, local_dir, remote_dir)
                    # t = threading.Thread(target=self.upload, args=(ip2, user, passwd, local_dir, remote_dir))
                else:
                    output('action is invalid', logType='telnet')
                    return
            # t.start()
        timeOut = 0
        while sshPool.aliveWorkers():
            if timeOut >= self._timeout:
                break
            time.sleep(0.5)
            timeOut += 0.5

    def popen(self, ip2, user, passwd, command):
        paramiko.util.log_to_file(self.logfile)
        ssh = self.connect2(ip2, user, passwd)
        if isinstance(command, type([1])):
            outs = []
            for m in command:
                try:
                    ot = self._exec_command(ssh, m)
                    outs.append(ot)
                except Exception, e:
                    output(ip2 + ' popen/m ' + m + str(e), logType='telnet')
                    continue
        else:
            try:
                outs = self._exec_command(ssh, command)
            except Exception, e:
                output(ip2 + ' popen ' + str(command) + str(e), logType='telnet')
                pass
        ssh.close()
        return outs

    def ssh(self, ip2, user, passwd, command):
        try:
            paramiko.util.log_to_file(self.logfile)
            ssh = self.connect2(ip2, user, passwd)
            fmt = '%-20s %9s'
            for m in command:
                try:
                    ots = self._exec_command(ssh, m)
                    if 'noout' not in argv_cli['dicts']:
                        print fmt % (ip2, '%s' % m)
                        print '\n'.join([fmt % ('', '%s' % ot) for ot in ots['out'] if ot])
                except Exception, e:
                    output(ip2 + ' ssh/m ' + m + str(e), logType='telnet')
                    pass
            ssh.close()
        except Exception, e:
            output(str(command) + ' ssh ' + str(e), logType='telnet')
            return

    def _exec_command(self, _ssh, m):
        ot = {}
        try:
            stdin, stdout, stderr = _ssh.exec_command(m, timeout=self._timeout)
            out = stdout.readlines()
            out = list(set(out))
            err = stderr.readlines()
            err = list(set(err))
            ot["cmd"] = m
            ot["out"] = []
            ot["err"] = []
            for o in out:
                ot["out"].append(o.strip())
            for e in err:
                ot["err"].append(e.strip())
        except Exception, e:
            output(m + ' _exec_command ' + str(e), logType='telnet')
            pass
        return ot

    def download(self, ip2, user, passwd, local_dir, remote_dir):

        paramiko.util.log_to_file(self.logfile)
        t = self.connect2(ip2, user, passwd, ctype='ftp')

        sftp = paramiko.SFTPClient.from_transport(t)

        files = sftp.listdir(remote_dir)
        for f in files:
            sftp.get(os.path.join(remote_dir, f), os.path.join(local_dir, f))
        t.close()

    def upload(self, ip2, user, passwd, local_dir, remote_dir):
        last_str = local_dir[-1:]
        if last_str == '/':
            local_dir = local_dir[0:-1]
        last_str = remote_dir[-1:]
        if last_str == '/':
            remote_dir = remote_dir[0:-1]
        if not local_dir or not remote_dir:
            output('local_dir or remote_dir is null ', logType='telnet')
            return None
        # master节点不上传
        iphost = ipaddress()
        if ip2 in iphost:
            output('The files of master node %s is not alowed to be covered ' % ip2, logType='telnet')
            return None
        try:
            paramiko.util.log_to_file(self.logfile)
            t = self.connect2(ip2, user, passwd, ctype='ftp')
            sftp = paramiko.SFTPClient.from_transport(t)
            def _file_path(_root, _filepath):
                _local = os.path.join(_root, _filepath)
                _name = _local.replace(local_dir + '/', '')
                _remote = os.path.join(remote_dir, _name)
                #日志目录下的文件不上传
                _log_file_dir = 0
                _name_list = _name.split('/')
                un_file_path = ['logs', 'tmp', 'ext.data']
                for up in _name_list:
                    if up in un_file_path:
                        if len(_name_list) > 1:
                            _log_file_dir = 2
                        else:
                            _log_file_dir = 1
                        break
                return _local, _remote, _log_file_dir
            for root, dirs, files in os.walk(local_dir):
                for filespath in files:
                    local_file, remote_file, log_file_dir = _file_path(root, filespath)
                    if log_file_dir > 0:
                        continue
                    if filespath == 'master':
                        continue
                    try:
                        sftp.put(local_file, remote_file)
                        if filespath == 'slave':
                            sftp.chmod(remote_file, 0777)
                    except Exception, e:
                        try:
                            sftp.mkdir(os.path.split(remote_file)[0])
                            sftp.put(local_file, remote_file)
                            if filespath == 'slave':
                                sftp.chmod(remote_file, 0777)
                        except Exception, e:
                            output(ip2 + ' upload.put.mkdir ' + local_file + ' ' + remote_file + ' ' + str(e), logType='telnet')
                            continue
                    #print "upload %s to remote %s" % (local_file, remote_file)
                for name in dirs:
                    _, remote_path, log_file_dir = _file_path(root, name)
                    if log_file_dir > 1:
                        continue
                    try:
                        sftp.mkdir(remote_path)
                    except Exception, e:
                        output(ip2 + ' upload.mkdir ' + remote_path + ' ' + str(e), logType='telnet')
                        continue

            t.close()
        except Exception, e:
            output((ip2, 'upload', e), logType='telnet')

    # pem key文件连接
    @staticmethod
    def connect2(host, username, passwd, ctype=''):
        if not passwd:
            if username == 'root':
                pkfile = '/root/.ssh/id_rsa'
            else:
                pkfile = '/home/%s/.ssh/id_rsa' % username
            mykey = paramiko.RSAKey.from_private_key_file(pkfile)
        elif passwd[-4:] == '.pem':
            pkfile = os.path.expanduser(passwd)
            mykey = paramiko.RSAKey.from_private_key_file(pkfile)
        if ctype == 'ftp':
            t = paramiko.Transport((host, 22))
            if passwd[-4:] == '.pem':
                t.connect(username=username, pkey=mykey)
            elif passwd:
                username = singleton.getinstance('pcode').decode(username)
                passwd = singleton.getinstance('pcode').decode(passwd)
                t.connect(username=username, password=passwd)
            else:
                t.connect(username=username, pkey=mykey)
            return t
        else:
            ssh = paramiko.SSHClient()
            #  首次登录自动yes
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if passwd[-4:] == '.pem':
                ssh.connect(host, username=username, pkey=mykey, timeout=5)
            elif passwd:
                username = singleton.getinstance('pcode').decode(username)
                passwd = singleton.getinstance('pcode').decode(passwd)
                ssh.connect(host, 22, username, passwd, timeout=5)
            else:
                ssh.load_system_host_keys()
                # 无密码登录
                ssh.connect(host, username=username, pkey=mykey, timeout=5)
            return ssh

