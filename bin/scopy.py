# -*- coding: utf-8 -*-
# Filename: scopy.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-07-25
# Author:       mpdesign
# description:  拷贝工程文件至slave
# -----------------------------------

from common.common import *


# 拷贝项目
def executeBin(params={}):
    if 'f' in params["dicts"].keys():
        ldir = params["dicts"]["f"]
        rdir = params["dicts"]["f"]
    else:
        ldir = PATH_CONFIG["project_path"]
        rdir = PATH_CONFIG["project_path"]
    if not ldir or ldir[0:len(PATH_CONFIG['project_path'])] != PATH_CONFIG['project_path']:
        output('Path is error, copy parent fold must be: %s' % PATH_CONFIG['project_path'])
        sys.exit(0)
    singleton.getinstance('ptelnet').ssh2(action='upload', host=SLAVE_NODE, local_dir=ldir, remote_dir=rdir)
    logger('Please wait for 3 sconds to copy the code to all slave node ', 'scopy')
    time.sleep(3)
    return True


