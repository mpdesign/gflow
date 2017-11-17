# -*- coding: utf-8 -*-
# Filename: activityCallbackFailed.py

# -----------------------------------
# Revision:         3.0
# Date:             2015-08-10
# Author:           mpdesign
# Website:          api.dc.737.com/da
# description:      失败回调
# frequency:        timely
# -----------------------------------

from streamInterface import *


class activityCallbackFailedTask(streamInterface):

    def beforeExcute(self):
        self.sleepExcute = 60

    def mapTask(self):
        return self.assignTask(byserver=False)

    #默认执行方法
    def excute(self, myTask=[]):
        for g in myTask:
            app_id = g['app_id']
            self.callbackFailed(app_id, 'd_activity_callback')

    # 定期执行失败的回调记录
    def callbackFailed(self, app_id, table):
        callback_list = db('ga_data', app_id).debug(mode=False, code=[1146]).query("select * from %s where callback_status=1 and callback_count<10" % table, "all")
        if emptyquery(callback_list):
            return
        for c in callback_list:
            api_url = c['callback_url']
            _id = c['id']
            _, status = self.doresult(app_id, c['channel_id'], api_url)
            db('ga_data', app_id).execute("update %s set `callback_count`=`callback_count`+1, callback_status=%s where id=%s" % (table, status, _id))
        db('ga_data', app_id).close()

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