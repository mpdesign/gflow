--*******************
--  更新角色在线信息脚本
--*******************
--  登录日志表
local login_log_name = KEYS[1]
local login_online_key_pre = 'xy_login_online_' .. app_id
local login_offline_key_pre = 'xy_login_' .. app_id
local i = 1
while true do
    --  当超过5次没取到数据时，则退出
    if i >= 5 then
        return 0
    end
    --  取出登录日志表
    local login_log = ''
    --  如果当前队列没数据，则阻塞(无法使用阻塞命令blpop)
    login_log = redis.call('lpop', login_log_name)
    if login_log then
        local r_log = cjson.decode(login_log)
        local pid = r_log['pid']
        local uid = r_log['uid']
        local did = r_log['did']
        local sid = r_log['sid']
        local channel_id = r_log['channel_id']
        local login_time = r_log['v_time']
        --  拼接在线表key
        local login_online_key = login_online_key_pre .. sid .. '_' .. channel_id
        local login_online_val = {}
        --  判断是否存在登录在线表中
        local is_exist = redis.call('hexists', login_online_key, pid)
        -- 该角色当前在线
        if is_exist ~= 0 then
            --  取出该角色在线信息
            local r_online = cjson.decode(redis.call('hget', login_online_key, pid))
            local end_time = r_online['end_time']
            if login_time > end_time then
                --  强制将记录插入登录离线表
                local login_offline_val = {}
                login_offline_val['sid'] = r_online['sid']
                login_offline_val['channel_id'] = r_online['channel_id']
                login_offline_val['pid'] = pid
                login_offline_val['uid'] = r_online['uid']
                login_offline_val['did'] = r_online['did']
                login_offline_val['start_time'] = r_online['start_time']
                login_offline_val['end_time'] = end_time
                --  根据登录时间获取插入对应日期的离线表
                login_offline_key = login_offline_key_pre .. os.date('%Y%m%d', r_online['start_time'])
                redis.call('zadd', login_offline_key, cjson.encode(login_offline_val), r_online['start_time'])

                --  在登录在线表重置为刚登录状态
                login_online_val['uid'] = uid
                login_online_val['did'] = did
                login_online_val['start_time'] = login_time
                login_online_val['end_time'] = login_time + 900
                login_online_val['last_login_time'] = login_time
                redis.call('hset', login_online_key, pid, cjson.encode(login_online_val))
            else
                --  更新登录时间与强制下线时间
                login_online_val['uid'] = r_online['uid']
                login_online_val['did'] = r_online['did']
                login_online_val['start_time'] = r_online['start_time']
                login_online_val['end_time'] = login_time + 900
                login_online_val['last_login_time'] = login_time
                redis.call('hset', login_online_key, pid, cjson.encode(login_online_val))
            end
        else
            --  该角色当前不在线
            login_online_val['uid'] = uid
            login_online_val['did'] = did
            login_online_val['start_time'] = login_time
            login_online_val['end_time'] = login_time + 900
            login_online_val['last_login_time'] = login_time
            --  将角色信息加入在线表
            redis.call('hset', login_online_key, pid, cjson.encode(login_online_val))
        end
    else
        i = i + 1
    end
end

