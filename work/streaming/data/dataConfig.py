# -*- coding: utf-8 -*-
# Filename: dataConfig.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-09-05
# Author:       mpdesign
# -----------------------------------


schemes = dict()

schemes['login'] = {
    'table': 'd_login',
    'fields': ["v_time", "v_hour", "v_day", "channel_id", "pid", "did", "uid", "sid", "ip", 'country_id', 'province_id', 'city_id'],
    'analysis_fields' : ['ip']
}

schemes['show'] = {
    'table': 'd_page',
    'fields': ['wid', 'adid', 'ip', 'channel_id', 'user_agent', 'unique_code',  'v_day', 'v_time'],
}

schemes['landing'] = {
    'table': 'd_landing',
    'fields': ['wid', 'adid', 'ip', 'channel_id', 'user_agent', 'unique_code',  'v_day', 'v_time'],
}

schemes['click'] = {
    'table': 'd_click',
    'fields': ['v_hour', 'v_day', 'v_time', 'mac', 'idfa', 'ip', 'channel_id', 'sid', 'adext', 'adid'],
}

schemes['activity'] = {
    'table': 'd_device',
    'fields': ['v_hour', 'v_day', 'v_time', 'did', 'screen', 'osv', 'hd', 'gv', 'mac', 'idfa', 'ip', 'channel_id', 'sid', 'isbreak', 'ispirated', 'adid', 'wid', 'country_id', 'province_id', 'city_id', 'ext'],
    'analysis_fields' : ['ip']
}

schemes['activityCallback'] = {
    'table': 'd_activity_callback',
    'fields': ['v_day', 'v_time', 'channel_id', 'callback_url', 'callback_result']
}

schemes['user'] = {
    'table': 'd_user',
    'fields': ["v_time", "v_hour", "v_day", "channel_id", "did", "uid", "username", "ip", "type", "gender", "adid", 'country_id', 'province_id', 'city_id'],
    'update': {'set': ['username', 'gender'], 'where': ['uid']},
    'analysis_fields' : ['ip'],
    'load_redis' : {'key_fields':'uid', 'val_fields':['v_time', 'sid', 'channel_id'], 'redis_tb':'base_user'}
}

schemes['player'] = {
    'table': 'd_player',
    'fields': ["v_time", "v_hour", "v_day", "channel_id", "did", "pid", "uid", "sid", "newdid", "pname", "level", "last_login_day", 'adid', 'country_id', 'province_id', 'city_id'],
    'update': {'set': ['level'], 'where': ['pid']},
    'analysis_fields' : ['ip'],
    'load_redis' : {'key_fields':'pid', 'val_fields':['v_time', 'sid', 'channel_id'], 'redis_tb':'base_player'}
}


schemes['record'] = {
    'table': 'd_record',
    'fields': ["v_time", "v_day", "channel_id", "pid", "level", "sid", "vip", "missionID", "itemID", "itemNum", "currencyID", "currencyNum", "currencyRemain", "itemRemain"],
    # 'tableMaxNum': 10000000
}

schemes['mission'] = {
    'table': 'd_mission',
    'fields': ["v_time", "v_day", "channel_id", "pid", "level", "sid", "missionID", "status", "level_1", "level_2", "level_3"],
}

schemes['event'] = {
    'table': 'd_event',
    'fields': ["v_time", "v_day", "channel_id", "pid", "did", "sid", "eventID", "value"],
}

schemes['excepter'] = {
    'table': 'd_exception',
    'fields': ["v_time", "v_day", "channel_id", "pid", "did", "e_name", "e_reason", "e_stack", "gv", "gv_build", "ip"]
}

schemes['playerLevel'] = {
        'table': 'd_player_level',
        'fields': ["v_time", "v_day", "pid", "level", "sid", "channel_id"],
        'check': True
}

schemes['iap'] = {
    'table': 'd_iap',
    'fields': ['v_time', 'v_day', 'did', 'oid', 'sid', 'channel_id', 'pid', 'info', 'key', 'value', 'ip'],
}

schemes['ip'] = {
    'fields' : ["ip"]
}