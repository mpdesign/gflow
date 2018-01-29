--******************
--  统计游戏次数、时长
--******************
local start_time = KEYS[1]
local end_time = KEYS[2]
local app_id = KEYS[3]
local login_offline_key = 'xy_login_' .. app_id
-- 获取昨日登录记录
login_list = get_login_dict(login_offline_key, start_time, end_time)
data = {}
for r in login_list do
    r = cjson.decode(r)
    pid = r['pid']
    if data[pid] == nil do
        data[pid] = {}
    end
    data[pid]['uid'] = r['uid']
    data[pid]['did'] = r['did']
    data[pid]['play_time'] += r['end_time'] - r['start_time']
    data[pid]['play_times'] += 1
end
return data

-- 获取时间范围内登录角色记录
local function get_login_dict(login_offline_key, start_time, end_time)
    login_list = redis.call('zrangebyscore', login_offline_key, start_time, end_time)
    return login_list

