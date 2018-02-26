# -*- coding: utf-8 -*-
# Filename: hive.py

# -----------------------------------
# Revision:     2.0
# Date:         2018-02-11
# Author:       mpdesign
# description:  hive sql
# -----------------------------------

from common.common import *


def executeBin(params={}):

    if 'schema' not in params['dicts']:
        print 'please input schema'
        return
    if len(params['argvs']) < 2:
        print 'Please input sql'
        return
    schema = params['dicts']['schema']
    sql = ' '.join(params['argvs']).rstrip(';')
    print 'HIVING [%s]: %s' % (schema, sql)
    start_time = time.time()
    res = singleton.getinstance('phive', 'core.db.phive').conn(
        host=HIVE_CONFIG['host'],
        port=HIVE_CONFIG['port'],
        username=HIVE_CONFIG['username'],
        schema=schema
    ).query(sql)
    row_len = len(res) if res else 0
    print 'Result: '
    print '+--------------------------+'
    print res
    print '+--------------------------+'
    print 'Time taken: %s seconds, Fetched: %s row(s)' % (time.time()-start_time, row_len)


