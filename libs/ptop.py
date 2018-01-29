# -*- coding: utf-8 -*-
# Filename: ptop.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-11-06
# Author:       mpdesign
# description:  监控脚本状态
# -----------------------------------

import paramiko
from common.common import *


class ptop:

    def __init__(self):
        self.taskList = []
        pass

    def show(self, tname='all'):
        self.script_status()

    def script_status(self):
        for h in SLAVE_NODE:
            ip2 = h["ip"]
            ssh_info = db().query("select * from %s where db='slave' and host='%s' limit 1" % (CONFIG_TABLE, ip2))
            if not ssh_info or not isinstance(ssh_info, type({})):
                output('%s has not been config in db' % ip2, logType='top')
                continue
            user = ssh_info['user']
            passwd = ssh_info['password']

            cmds = []
            cmd = 'ps aux  | grep "%s" | grep  -v "grep"' % PATH_CONFIG["project_path"]
            cmds.append(cmd)
            out = singleton.getinstance('ptelnet').popen(ip2, user, passwd, cmds)

            ocmds = {}
            for o in out:
                for oo in o['out']:
                    if oo:
                        oarrs = []
                        oarr = oo.split(' ')
                        for ocmd in oarr:
                            if ocmd and ocmd != ' ':
                                oarrs.append(ocmd)

                        oarr = oarrs
                        for ocmd in oarr:
                            if ocmd in self._task_list():
                                break
                        if not ocmd:
                            continue
                        #进程ID
                        pid = oarr[1]

                        if ocmd not in ocmds.keys():
                            ocmds[ocmd] = {}
                        if 'NUM' not in ocmds[ocmd].keys():
                            ocmds[ocmd]['NUM'] = 0
                        #虚拟内存
                        ocmds[ocmd]['VSZ'] = oarr[4]
                        #物理内存
                        ocmds[ocmd]['RSS'] = oarr[5]
                        #启动时间
                        ocmds[ocmd]['START'] = oarr[8]
                        ocmds[ocmd]['TIME'] = oarr[9]
                        ocmds[ocmd]['NUM'] += 1
                        #线程数
                        if "THREAD" not in ocmds[ocmd].keys():
                            tcmd = 'pstree -pa %s |grep -v "%s"| wc -l' % (pid, ocmd)
                            tout = singleton.getinstance('ptelnet').popen(ip2, user, passwd, [tcmd])
                            try:
                                ocmds[ocmd]['THREAD'] = tout[0]['out'][0]
                            except Exception, e:
                                ocmds[ocmd]['THREAD'] = 0
            fmt = '%-15s %8s %8s %8s %8s %8s %8s'
            print fmt % (ip2, 'NUM', 'START', 'TIME', 'VSZ', 'RSS', 'THREAD')
            for oc in ocmds:
                vsz = "%sM" % round(floatval(ocmds[oc]['VSZ'])/float(1024*1024), 3)
                rss = "%sM" % round(floatval(ocmds[oc]['RSS'])/float(1024*1024), 3)
                msg = fmt % (oc, '[%s]' % ocmds[oc]['NUM'], ocmds[oc]['START'], ocmds[oc]['TIME'], vsz, rss, ocmds[oc]['THREAD'])
                print '\n', msg
            print '\n'

    def _task_list(self):
        if not self.taskList:
            task_path = "%s/task" % PATH_CONFIG['project_path']
            for parent, dirnames, filenames in os.walk(task_path):
                for dirname in dirnames:
                    self.taskList.append(dirname)
        return self.taskList







