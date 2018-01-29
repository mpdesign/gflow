# -*- coding: utf-8 -*-
# Filename: inspect.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-11-20
# Author:       mpdesign
# description:  任务检查器
# -----------------------------------

from common.common import *


# 拷贝项目
def executeBin(params={}):
    inspect_path = '%s/inspect' % PATH_CONFIG['tmp_path']
    inspect_files = os.listdir(inspect_path)
    if len(params['argvs']) < 3:
        print '  '.join(inspect_files)
    else:
        inspect_file = '%s/%s.ins' % (inspect_path, params['argvs'][2].replace('.ins', ''))
        if singleton.getinstance('pfile').isfile(inspect_file):
            os.system('cat %s' % inspect_file)
        else:
            ins_files = []
            for f in inspect_files:
                if params['argvs'][2] == f[0:len(params['argvs'][2])]:
                    ins_files.append(f)
            if not ins_files:
                ins_files = inspect_files
            print '  '.join(ins_files)


