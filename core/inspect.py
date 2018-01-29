# -*- coding: utf-8 -*-
# Filename: inspect.py

# -----------------------------------
# Revision:         2.0
# Date:             2018-01-11
# Author:           mpdesign
# description:      任务检查
# -----------------------------------

from comm.common import *
import inspect as inspectOrigin


class inspect:

    @staticmethod
    def getsource(package=''):
        exec("import %s" % package)
        return inspectOrigin.getsource(package)

    @staticmethod
    def read(taskFullName='', ipnode='', usehdfs=True):
        f = '%s/inspect/%s.ins' % (PATH_CONFIG['tmp_path'], taskFullName)
        if not usehdfs:
            if not singleton.getinstance('pfile').isfile(f):
                return False
            fp = open(f, 'r')
            data = singleton.getinstance('pjson').loads(fp.read())
            fp.close()
        else:
            ipnode = ipnode or curNode()
            hdfs_inspect_path = "%s%s/inspect/%s" % (PATH_CONFIG['hdfs_home'], PATH_CONFIG['tmp_path'], ipnode)
            data = singleton.getinstance('phdfs', 'core.db.phdfs')\
                .setfile("%s/%s.ins" % (hdfs_inspect_path, taskFullName), delimiter='')\
                .read()
            data = singleton.getinstance('pjson').loads(data)
        return data

    @staticmethod
    def write(taskFullName='', data={}, usehdfs=True):
        content = singleton.getinstance('pjson').dumps(data, indent=4)
        if not usehdfs:
            f = '%s/inspect/%s.ins' % (PATH_CONFIG['tmp_path'], taskFullName)
            singleton.getinstance('pfile').mkdirs(f, True)
            fp = open(f, 'w')
            fp.write(content)
            fp.close()
        # else:
        #     hdfs_inspect_path = "%s%s/inspect/%s" % (PATH_CONFIG['hdfs_home'], PATH_CONFIG['tmp_path'], curNode())
        #     try:
        #         singleton.getinstance('phdfs', 'core.db.phdfs')\
        #             .write(data=content, filepath="%s/%s.ins" % (hdfs_inspect_path, taskFullName), append=False, overwrite=True, delimiter='')
        #     except Exception, e:
        #         singleton.getinstance('phdfs', 'core.db.phdfs')\
        #             .mkdirs(hdfs_inspect_path)\
        #             .write(data=content, filepath="%s/%s.ins" % (hdfs_inspect_path, taskFullName), append=False, overwrite=True, delimiter='')

    @staticmethod
    def delete(taskFullName=''):
        hdfs_inspect_file = "%s%s/inspect/%s/%s.ins" % (PATH_CONFIG['hdfs_home'], PATH_CONFIG['tmp_path'], curNode(), taskFullName)
        singleton.getinstance('phdfs', 'core.db.phdfs').delete(hdfs_inspect_file)