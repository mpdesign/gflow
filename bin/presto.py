# -*- coding: utf-8 -*-
# Filename: presto.py

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
    if 'catalog' not in params['dicts']:
        print 'please input catalog [mysql|hive]'
        return
    if len(params['argvs']) < 2:
        print 'Please input sql'
        return
    schema = params['dicts']['schema']
    catalog = params['dicts']['catalog']
    username = PRESTO_CONFIG['hiveusername'] if catalog == 'hive' else None
    sql = ' '.join(params['argvs']).rstrip(';')
    print 'PRESTO [%s@%s]: %s' % (schema, catalog, sql)
    start_time = time.time()
    res = singleton.getinstance('prestodb', 'core.db.prestodb').conn(
        host=PRESTO_CONFIG['host'],
        port=PRESTO_CONFIG['port'],
        catalog=catalog,
        username=username,
        schema=schema
    ).query(sql)
    row_len = len(res) if res else 0
    print 'Result: '
    print '+--------------------------+'
    print res
    print '+--------------------------+'
    print 'Time taken: %s seconds, Fetched: %s row(s)' % (time.time()-start_time, row_len)


