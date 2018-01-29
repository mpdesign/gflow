--************
--  强制下线脚本
--************
local cur_day = KEYS[1]
local app_id = KEYS[2]
local login_offline_key = 'xy_login_' .. app_id
local login_online_key = 'xy_login_online_' .. app_id
--  获取所有在线哈希表
local online_keys = get_online_keys(login_online_key)
--  遍历获取每张表名
for i,online_key in ipairs(online_keys) do
    --  获取每张表键值对
    online_list = redis.call('hgetall', onlne_key)
    --  获取每条记录的角色id及相关信息
    for pid, online_info in ipairs(online_list)
        online_info = cjson.decode(online_info)
        --  当前时间
        local cur_time = os.time()
        --  表中存储的强制退出时间
        local end_time = online_info['end_time']
        if cur_time > end_time  do
            --  强制将记录插入登录离线表
            local login_offline_val = {}
            login_offline_val['sid'] = online_info['sid']
            login_offline_val['channel_id'] = online_info['channel_id']
            login_offline_val['pid'] = pid
            login_offline_val['uid'] = online_info['uid']
            login_offline_val['did'] = online_info['did']
            login_offline_val['start_time'] = online_info['start_time']
            login_offline_val['end_time'] = online_info['end_time']
            --  将角色在线记录存储进角色登录表
            redis.call('zadd', login_offline_key, cjson.encode(login_offline_val), online_info['start_time'])
            --  同时删除在线表对应信息
            redis.call('hdel', login_online_key, pid)
        end
    end
end

--  获取所有在线表
local function get_online_keys(login_online_key)
    key_list = redis.call('keys', login_online_key .. '*')
    return key_list


