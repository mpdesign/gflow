# -*- coding: utf-8 -*-
# Filename: alterTable.py

# -----------------------------------
# Revision:         2.0
# Date:             2016-10-25
# Author:           mpdesign
# description:      更新数据表结构
# frequency:        sleepExecute
# -----------------------------------

from monitor import *


class alterTableTask(monitorJob):

    def beforeExecute(self):
        self.breakExecute = True

    def taskDataList(self):
        return DEFAULT_NODE

    def execute(self, myTaskDataList=[]):
        gs = self.assignTask(byserver=False)
        for g in gs:
            app_id = g['app_id']
            # db(db_type='ga_reporter', app_id=app_id).query("alter table r_ad_summary add `v_a2` int(10) unsigned NOT NULL COMMENT '次日留存'");
            # db(db_type='ga_reporter', app_id=app_id).query("alter table r_ad_summary add `v_a7` int(10) unsigned NOT NULL COMMENT '7日留存'");
            # db(db_type='ga_reporter', app_id=app_id).query("alter table r_ad_summary add `v_a14` int(10) unsigned NOT NULL COMMENT '14日留存'");
            # db(db_type='ga_reporter', app_id=app_id).query("alter table r_ad_summary add `v_a30` int(10) unsigned NOT NULL COMMENT '30日留存'");

            # db(db_type='ga_reporter', app_id=app_id).query("alter table r_pay_value add `day1` int(10) DEFAULT NULL COMMENT '1日后充值总金额'");
            # db(db_type='ga_reporter', app_id=app_id).query("alter table r_pay_value add `day3` int(10) DEFAULT NULL COMMENT '3日后充值总金额'");
            # db(db_type='ga_reporter', app_id=app_id).query("alter table r_pay_value add `v_p1` int(10) NOT NULL COMMENT '1日付费玩家数'");
            # db(db_type='ga_reporter', app_id=app_id).query("alter table r_pay_value add `v_p3` int(10) NOT NULL COMMENT '3日付费玩家数'");

            # db().query("alter table ga_t_r_pay_value add `day1` int(10) DEFAULT NULL COMMENT '1日后充值总金额'")
            # db().query("alter table ga_t_r_pay_value add `day3` int(10) DEFAULT NULL COMMENT '3日后充值总金额'")
            # db().query("alter table ga_t_r_pay_value add `v_p2` int(10) NOT NULL COMMENT '1日付费玩家数'")
            # db().query("alter table ga_t_r_pay_value add `v_p3` int(10) NOT NULL COMMENT '3日付费玩家数'")
        print 'complete alterTable'
