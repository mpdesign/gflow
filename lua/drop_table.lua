-- 撤销表

local db_key = KEYS[1]
local table_key = KEYS[2]

local db_table_key = db_key .. "." .. table_key .. ".*"

local db_table_keys = redis.call("keys", db_table_key)
if db_table_keys and #db_table_keys > 0 then
    for i in pairs(db_table_keys) do
        -- 设置过期时间
        redis.call("expire", db_table_keys[i], 1)
    end
end
-- 删除配置
redis.call("expire", "lua.config", 1)
return true
