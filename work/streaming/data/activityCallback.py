# -*- coding: utf-8 -*-
# Filename: activityCallback.py

# -----------------------------------
# Revision:         3.0
# Date:             2015-08-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      激活回调接口
# frequency:        timely
# -----------------------------------

from dataInterface import *
from dataConfig import *


class activityCallbackTask(dataInterface):

    #默认执行方法
    def execute(self, myTask=[]):

        self.mutiWorker(myTask=myTask, schemes=schemes['activityCallback'], popKeyPre='callback')

    def doWorker(self, popKey, schemes):

        # 取出数据
        rows_insert, rows_update = dataModel.popData(popKey, schemes)
        # 处理数据
        self.callbackApi(rows_insert, schemes['table'])

    def callbackApi(self, rows_insert, table):
        if not rows_insert:
            return
        for app_id in rows_insert:
            values = []
            for r in rows_insert[app_id]:
                cdata = dict()
                cdata['channel_id'] = itemDict(r, 'channel_id')
                cdata['callback_url'] = itemDict(r, 'callback_url')
                cdata['v_time'] = itemDict(r, 'v_time')
                if intval(cdata['v_time']) > 0:
                    cdata['v_day'] = time.strftime('%Y%m%d', time.localtime(cdata['v_time']))
                cdata['callback_time'] = time.time()
                cdata['callback_day'] = time.strftime('%Y%m%d', time.localtime(cdata['callback_time']))
                if cdata['callback_url']:
                    # 回调返回数据
                    cdata['callback_result'], cdata['callback_status'] = self.doresult(app_id, cdata['channel_id'], cdata['callback_url'])
                values.append(cdata)
            # 记录回调日志
            db_save_data(table=table, data=values, app_id=app_id)

    def doresult(self, app_id, channel_id, callback_url):
        channel_info = self.channel(app_id, channel_id=channel_id)
        if not callback_url or emptyquery(channel_info):
            return '', False

        # 回调返回数据
        # 头信息
        callback_header = channel_info['ch_callback_api_header']
        if callback_header:
            _header = {}
            for h in callback_header.split('\n'):
                if not h:
                    continue
                h = h.split(':')
                _header[h[0]] = '' if len(h) < 2 else h[1]
            callback_header = _header
        # body数据
        if channel_info['ch_callback_api_method'] == 'post':
            callback_params = callback_url.split('?')
            callback_data = callback_params[1] if len(callback_params) > 1 else None
            callback_url = callback_params[0]
            if channel_info['ch_callback_api_body'] == 'json':
                if callback_data:
                    jsondata = {}
                    for p in callback_data.split('&'):
                        if not p:
                            continue
                        p = p.split('=')
                        jsondata[p[0]] = '' if len(p) < 2 else p[1]
                    callback_data = singleton.getinstance('pjson').dumps(jsondata)
            elif channel_info['ch_callback_api_body'] and channel_info['ch_callback_api_body'] != 'url':
                callback_data = channel_info['ch_callback_api_body']
        else:
            callback_data = None
        callback_res, ok = singleton.getinstance('phttp').file_get_contents(callback_url, getstatus=True, data=callback_data, headers=callback_header)

        callback_result = callback_res
        if ok:
            try:
                # 数据库查询 返回结果处理方式配置
                doresult = itemDict(channel_info, 'ch_callback_api_doresult')
                doresult = singleton.getinstance('pjson').loads(doresult)
                api_format = itemDict(doresult, 'format')
                # 成功条件
                success_expr = itemDict(doresult, 'success')
                if not success_expr:
                    exprs = True
                else:
                    # json格式
                    callback_res = singleton.getinstance('pjson').loads(callback_res) if api_format == 'json' else callback_res
                    orexpr = True
                    andexpr = True
                    if "or" in success_expr.keys():
                        orexpr = False
                        for se in success_expr["or"]:
                            callback_value = itemDict(callback_res, se['key']) if api_format == 'json' else callback_res
                            orexpr = orexpr or self.expr(callback_value, se['expr'], se['value'])
                    if "and" in success_expr.keys():
                        for se in success_expr["and"]:
                            callback_value = itemDict(callback_res, se['key']) if api_format == 'json' else callback_res
                            andexpr = andexpr and self.expr(callback_value, se['expr'], se['value'])
                    exprs = orexpr and andexpr
                callback_status = 0 if exprs else 1
            except Exception, e:
                callback_result = e
                callback_status = 1
        else:
            callback_status = 1
        return callback_result, callback_status

    @staticmethod
    def expr(v1='', expr='', v2=''):
        # 数字则整型比对
        if str.isdigit(str(v2)):
            v1 = intval(v1)
            v2 = intval(v2)

        if expr == '==':
            expr_result = v1 == v2
        elif expr == '!=':
            expr_result = v1 != v2
        elif expr == '>':
            expr_result = v1 > v2
        elif expr == '>=':
            expr_result = v1 >= v2
        elif expr == '<':
            expr_result = v1 < v2
        elif expr == '<=':
            expr_result = v1 <= v2
        else:
            expr_result = False
        return expr_result