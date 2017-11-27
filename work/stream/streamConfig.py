# -*- coding: utf-8 -*-
# Filename: py

# -----------------------------------
# Revision:     2.0
# Date:         2017-09-05
# Author:       mpdesign
# -----------------------------------


schemes = dict()

schemes['show'] = {
    'table': 'd_page',
    'fields': ['wid', 'adid', 'ip', 'channel_id', 'user_agent', 'unique_code',  'v_day', 'v_time'],
    'check': True
}

schemes['click2'] = {
    'table': 'd_click2',
    'fields': ['wid', 'adid', 'ip', 'channel_id', 'user_agent', 'unique_code',  'v_day', 'v_time'],
    'check': True
}

schemes['click'] = {
    'table': 'd_click',
    'fields': ['v_hour', 'v_day', 'v_time', 'mac', 'idfa', 'ip', 'channel_id', 'sid', 'adext', 'adid'],
    'check': True
}

schemes['activity'] = {
    'table': 'd_device',
    'fields': ['v_hour', 'v_day', 'v_time', 'did', 'screen', 'osv', 'hd', 'gv', 'mac', 'idfa', 'ip', 'newdid', 'channel_id', 'sid', 'isbreak', 'ispirated', 'adid', 'wid']
}

schemes['activityCallback'] = {
    'table': 'd_activity_callback',
    'fields': ['v_day', 'v_time', 'channel_id', 'callback_url', 'callback_result']
}

schemes['user'] = {
    'table': 'd_user',
    'fields': ["v_time", "v_hour", "v_day", "channel_id", "did", "uid", "username", "ip", "type", "area", "gender", 'adid'],
    'update': {'set': ['username', 'area', 'gender'], 'where': ['uid']}
}

schemes['player'] = {
    'table': 'd_player',
    'fields': ["v_time", "v_hour", "v_day", "channel_id", "did", "pid", "uid", "sid", "newdid", "pname", "level", "last_login_day", 'adid'],
    'update': {'set': ['level'], 'where': ['pid']}
}


schemes['record'] = {
    'table': 'd_record',
    'fields': ["v_time", "v_day", "channel_id", "pid", "level", "sid", "vip", "missionID", "itemID", "itemNum", "currencyID", "currencyNum", "currencyRemain", "itemRemain"],
    'check': True,
    'tableMaxNum': 10000000
}

schemes['mission'] = {
    'table': 'd_mission',
    'fields': ["v_time", "v_day", "channel_id", "pid", "level", "sid", "missionID", "status", "level_1", "level_2", "level_3"],
    'check': True
}

schemes['event'] = {
    'table': 'd_event',
    'fields': ["v_time", "v_day", "channel_id", "pid", "did", "sid", "eventID", "value"],
    'check': True
}

schemes['excepter'] = {
    'table': 'd_exception',
    'fields': ["v_time", "v_day", "channel_id", "pid", "did", "e_name", "e_reason", "e_stack", "gv", "gv_build", "ip"]
}

schemes['iap'] = {
    'table': 'd_iap',
    'fields': ['v_time', 'v_day', 'did', 'oid', 'sid', 'channel_id', 'pid', 'info', 'key', 'value', 'ip'],
    'check': True
}