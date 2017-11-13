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
        self.logfile = '%s/telnet.log' % path_config["log_path"]
        self._timeout = None
        pass

    def timeout(self, timeout=None):
        self._timeout = timeout
        return self

    def ssh2(self, **kwargs):
        if not kwargs["host"]:
            logger('host is null', 'telnet')
            return
        if not kwargs["action"]:
            logger('action is null', 'telnet')
            return
        sshPool = WorkerManager(len(kwargs["host"]))
        sshPool.parallel_for_complete().stop()
        for h in kwargs["host"]:
            ip2 = h["ip"]
            conn = singleton.getinstance('mysql', 'core.db.mysql').conn(db_config['ga_center']['host'], db_config['ga_center']['user'], db_config['ga_center']['password'], db_config['ga_center']['db'], db_config['ga_center']['port'])
            ssh_info = conn.query("select * from ga_db where db='ga_ssh' and host='%s' limit 1" % ip2)
            conn.close()
            if not ssh_info or not isinstance(ssh_info, type({})):
                logger('%s has not been config in db' % ip2, 'telnet')
                continue
            if ssh_info['password'][-4:] == '.pem':
                user = ssh_info['user']
                passwd = ssh_info['password']
            else:
                user = singleton.getinstance('pcode').decode(ssh_info['user'])
                passwd = singleton.getinstance('pcode').decode(ssh_info['password'])
            if not ip2 or not user or not passwd:
                logger('ip or user or passwd is null', 'telnet')
                return

            if kwargs["action"] == 'ssh':
                if not kwargs["command"]:
                    logger('command is null', 'telnet')
                    return
                sshPool.add(self.ssh, ip2, user, passwd, kwargs["command"])
                # t = threading.Thread(target=self.ssh, args=(ip2, user, passwd, kwargs["command"]))
            else:
                if not kwargs["local_dir"] or not kwargs["remote_dir"]:
                    logger('command is null', 'telnet')
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
                    logger('action is invalid', 'telnet')
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
                    logger(ip2 + ' popen/m ' + m + str(e), 'telnet')
                    continue
        else:
            try:
                outs = self._exec_command(ssh, command)
            except Exception, e:
                logger(ip2 + ' popen ' + str(command) + str(e), 'telnet')
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
                    print fmt % (ip2, '%s' % m)
                    print '\n'.join([fmt % ('', '%s' % ot) for ot in ots['out'] if ot])
                except Exception, e:
                    logger(ip2 + ' ssh/m ' + m + str(e), 'telnet')
                    pass
            ssh.close()
        except Exception, e:
            logger(str(command) + ' ssh ' + str(e), 'telnet')
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
            logger(m + ' _exec_command ' + str(e), 'telnet')
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
            logger('local_dir or remote_dir is null ', 'telnet')
            return None
        # master节点不上传
        iphost = ipaddress()
        if ip2 in iphost:
            logger('The files of master node %s is not alowed to be covered ' % ip2, 'telnet')
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
                un_file_path = ['logs', 'pids', 'tmp', 'ext.data']
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
                            logger(ip2 + ' upload.put.mkdir ' + local_file + ' ' + remote_file + ' ' + str(e), 'telnet')
                            continue
                    #print "upload %s to remote %s" % (local_file, remote_file)
                for name in dirs:
                    _, remote_path, log_file_dir = _file_path(root, name)
                    if log_file_dir > 1:
                        continue
                    try:
                        sftp.mkdir(remote_path)
                    except Exception, e:
                        logger(ip2 + ' upload.mkdir ' + remote_path + ' ' + str(e), 'telnet')
                        continue

            t.close()
        except Exception, e:
            logger((ip2, 'upload', e), 'telnet')

    # pem key文件连接
    @staticmethod
    def connect2(host, username, passwd, ctype=''):
        if ctype == 'ftp':
            t = paramiko.Transport((host, 22))
            if passwd[-4:] == '.pem':
                privatekeyfile = os.path.expanduser(passwd)
                mykey = paramiko.RSAKey.from_private_key_file(privatekeyfile)
                t.connect(username=username, pkey=mykey)
            else:
                t.connect(username=username, password=passwd)
            return t
        else:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if passwd[-4:] == '.pem':
                privatekeyfile = os.path.expanduser(passwd)
                mykey = paramiko.RSAKey.from_private_key_file(privatekeyfile)
                ssh.connect(host, username=username, pkey=mykey, timeout=5)
            else:
                ssh.connect(host, 22, username, passwd, timeout=5)
            return ssh

