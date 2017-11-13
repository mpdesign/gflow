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
        ldir = path_config["gf_path"]
        rdir = path_config["gf_path"]
    if not ldir or ldir[0:len(path_config['gf_path'])] != path_config['gf_path']:
        output('Path is error, copy parent fold must be: %s' % path_config['gf_path'])
        sys.exit(0)
    singleton.getinstance('ptelnet').ssh2(action='upload', host=slave_node, local_dir=ldir, remote_dir=rdir)
    logger('Please wait for 3 sconds to copy the code to all slave node ', 'scopy')
    time.sleep(3)
    return True


