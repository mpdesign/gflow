# -*- coding: utf-8 -*-
# Filename: playerLevel.py

# -----------------------------------
# Revision:         1.0
# Date:             2017-12-12
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      pop playerLevel data from redis to mysql
# frequency:        timely
# -----------------------------------

from dataInterface import *
from dataConfig import *

class playerLevelTask(dataInterface):

    #默认执行方法
    def excute(self, taskDataList=[]):

        self.mutiWorker(myTask=taskDataList, schemes=schemes['playerLevel'], popKeyPre='player_level')

    def doWorker(self, popKey, schemes):
        app_ids = {}
        # 取出数据
        rows_insert, _ = dataModel.popData(popKey, schemes)
        if rows_insert:
            for app_id in rows_insert:
                app_ids[app_id] = 1
                values = []
                # 获取最新可插入表
                table = checkTable(app_id, schemes)
                # 获取所有表
                tables = subTableList(app_id, tablename=schemes['table'], order='desc')
                # 分析等级报表
                for data in rows_insert[app_id]:
                    # 只分析10级以上的玩家
                    curlevel = intval(data['level'])
                    if curlevel < 10:
                        continue
                    bdata = self.levelReporter(app_id, data, tables)
                    if bdata:
                        values.append(bdata)
                db_save_data(table=table, app_id=app_id, data=values)
            for app_id in app_ids:
                db('ga_data', app_id).close()

    # 等级level升级记录 以及 上一级lastLevel玩家等级状态：游戏时长、游戏次数、虚拟币消费、购入、充值金额
    @staticmethod
    def levelReporter(app_id, data, tables):
        # 更新上一级玩家状态
        # 定位记录，获取最近1级记录
        lastLevel = intval(data['level']) - 1
        ptime = 0
        ptimes = 0
        pdays = 0

        bdata = {}
        # 汇总之前所有等级的游戏时长和次数
        for t in tables:
            sumpt = db('ga_data', app_id).query("select sum(play_time) as ptime, sum(play_times) as ptimes, sum(play_days) as pdays from %s where pid='%s'" % (t, data['pid']))
            if not emptyquery(sumpt):
                ptime += intval(sumpt['ptime'])
                ptimes += intval(sumpt['ptimes'])
                pdays += intval(sumpt['pdays'])
        # 游戏时长, 游戏次数
        bdata['play_time'] = 0
        bdata['play_times'] = 0
        result = db('ga_data', app_id).query("select play_time, play_times, play_days from d_player where pid='%s' limit 1" % data['pid'])
        if not emptyquery(result):
            bdata['play_time'] = intval(result['play_time']) - ptime
            bdata['play_times'] = intval(result['play_times']) - ptimes
            bdata['play_days'] = intval(result['play_days']) - pdays

        # 虚拟币消费、购入
        # 暂时不分析游戏记录
        if False:
            bdata['currencyConsume'] = 0
            bdata['currencyBuy'] = 0
            record_tables = subTableList(app_id, tablename='d_record', order='desc')
            for rt in record_tables:
                sumconsume = db('ga_data', app_id).query("select sum(currencyNum) as currencyConsume from %s where pid='%s' and level=%s and currencyNum < 0" % (rt, data['pid'], lastLevel))
                sumbuy = db('ga_data', app_id).query("select sum(currencyNum) as currencyBuy from %s where pid='%s' and level=%s and currencyNum > 0" % (rt, data['pid'], lastLevel))
                if not emptyquery(sumconsume):
                    bdata['currencyConsume'] += intval(sumconsume['currencyConsume'])
                if not emptyquery(sumbuy):
                    bdata['currencyBuy'] += intval(sumbuy['currencyBuy'])

        # 充值金额
        bdata['v_amount'] = 0
        bdata['v_recharge_c'] = 0
        result = db('ga_data', app_id).query("select sum(amount) as v_amount, count(1) as v_recharge_c  from d_pay where player_id='%s' and player_level=%s " % (data['pid'], lastLevel))
        if not emptyquery(result):
            bdata['v_amount'] = intval(result['v_amount'])
            bdata['v_recharge_c'] = intval(result['v_recharge_c'])

        # 插入上一级记录
        bdata['v_time'] = data['v_time']
        bdata['v_day'] = data['v_day']
        bdata['pid'] = data['pid']
        bdata['level'] = lastLevel
        bdata['sid'] = data['sid']
        bdata['channel_id'] = data['channel_id']
        return bdata