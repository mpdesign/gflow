# -*- coding: utf-8 -*-
# Filename: ip.py

# -----------------------------------
# Revision:         1.0
# Date:             2017-12-12
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      ip
# frequency:        timely
# -----------------------------------

from dataInterface import *
from dataConfig import *
from dataModel import *

class ipTask(dataInterface):

    #默认执行方法
    def execute(self, taskDataList=[]):
        # 测试
        # data_ip = {}
        # data_ip['app_id'] = 20001
        # data_ip['ip'] = '218.85.126.226'
        # data_ip['v_time'] = time.time()
        # memory(redisConfig(redis_type='data', app_id='20001')).redisInstance().rpush('xy_data_queue_ip_20001', singleton.getinstance('pjson').dumps(data_ip))
        self.mutiWorker(myTask=taskDataList, schemes=schemes['ip'], popKeyPre='ip')

    @staticmethod
    def doWorker(popKey, schemes):
        # 取出数据
        rows_insert, rows_update = dataModel.popData(popKey, schemes)
        for app_id in rows_insert:
            for row in rows_insert[app_id]:
                ip = row['ip']
                ip_int = int(socket.inet_aton(ip).encode('hex'),16)
                if memory(redisConfig(redis_type='device')).redisInstance().hexists("%sip_to_area" % (DB_PREFIX), ip_int) == False:
                    # 获取获取相应地区id串（形如：{1234567891234567:1_2_3}）
                    area_str = getFormatArea(MAXMIND_DB_CONFIG['path_city'], 'city', ip_int)
                    memory(redisConfig(redis_type='device')).redisInstance().hset("%sip_to_area" % (DB_PREFIX), ip_int, area_str)

