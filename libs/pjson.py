# -*- coding: utf-8 -*-

import json
import cPickle


class pjson:

    def __init__(self):
        pass

    @staticmethod
    def json():
        return json

    @staticmethod
    def loads(strs='', t='', encoding=None, object_hook=None):
        if not strs:
            return None
        try:
            if t == 'cPickle':
                r = cPickle.loads(strs)
            else:
                r = json.loads(strs, encoding=encoding, object_hook=object_hook)
        except Exception, e:
            return None
        return r

    @staticmethod
    def dumps(obj=None, t='', indent=0):
        if not obj:
            return None
        try:
            if t == 'cPickle':
                r = cPickle.dumps(obj)
            else:
                r = json.dumps(obj, indent=indent)
        except Exception, e:
            return None
        return r