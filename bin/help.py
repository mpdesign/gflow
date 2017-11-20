# -*- coding: utf-8 -*-
# Filename: help.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-03-17
# Author:       mpdesign
# description:  查看帮助
# -----------------------------------

from config.config import *


#--help
def executeBin(params):
    print "\n"
    print "description: must be running on the master node\n"
    print "usage: master <action> [layer.job] -option \n"
    print "action: <start|stop|restart|scopy|top|pcode|inspect>\n"
    print "option: \n"
    print "-d int date range for rerunning , split by ',' , example 20140909,20141009\n"
    print "-t register tasker , split by ',' \n"
    print "-g assign app_id \n"
    print "-now not wait to start  \n"
    print "-all run all job  \n"
    print "-f scopy file path, example '%s'\n" % PATH_CONFIG['project_path']
    print "\n"
