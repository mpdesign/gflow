# -*- coding: utf-8 -*-
# Filename: layer.py

# -----------------------------------
# Revision:         2.0
# Date:             2017-11-03
# Author:           mpdesign
# description:      批量管理作业层服务
# -----------------------------------

from comm.common import *


class layer():

    def __init__(self, layerName=''):
        self.layerName = layerName
        self.layerObj = self.importLayer()

    def importLayer(self):
        if not self.layerName:
            output('Layer name is not exists', log_type='system')
            return None
        layer_file = '%s/work/%s/%s.py' % (path_config['project_path'], self.layerName, self.layerName)
        if singleton.getinstance('pfile').isfile(layer_file):
            pkg_name = 'work.%s.%s' % (self.layerName, self.layerName)
        else:
            output('Layer package file %s is not exists' % self.layerName, log_type='system')
            return None
        import_layer = "from %s import *" % pkg_name
        exec(import_layer)
        layer_class_name = '%sLayer' % self.layerName
        layer_class = eval(layer_class_name)
        newlayer = layer_class()
        return newlayer

    def stop(self):
        self.run('stop')

    def start(self):
        self.run('start')

    def restart(self):
        self.run('restart')

    def run(self, action=''):
        print '\n'
        ps = ''
        for p in argv_cli["dicts"]:
            ps = "%s -%s %s" % (ps, p, argv_cli["dicts"][p])
        for jobName in self.layerObj.registerJob:
            output('Job %s starting ...' % jobName)
            cmd = "%s/slave %s %s %s" % (path_config["project_path"], action, self.layerName + '.' + jobName, ps)
            os.system(cmd)

        output('All job has %sed ' % action, log_type='system')
