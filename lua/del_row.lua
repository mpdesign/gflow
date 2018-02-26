-- 根据索引删除记录，仅支持单个索引条件的删除操作
-- 1.索引引擎 bitmap hash index
-- 2.索引类型 in range or
-- 3.查找rowid，并删除对应索引
-- 4.rowid合并，并删除对应哈希表记录,
-- 5.rowid反向删除关联索引
-- 注意：通过单个rowid条件直接删除记录，必须先查询出rowid关联的所有索引，再删除之

local db_key = KEYS[1]
local table_key = KEYS[2]
local config = redis.call('get', "lua.config")
if config then
    config = cjson.decode(config)
else
    return "not config file"
end
local get_config = function(k)
    if config["table." .. table_key] and config["table." .. table_key][k] then
        return config["table." .. table_key][k]
    end
    return config[k]
end
local index_zset_field_len_limit = get_config("index_zset_field_len_limit")
local row_page_range = get_config("row_page_range")
local row_block_delimiter = get_config("row_block_delimiter")
local row_len_limit = get_config("row_len_limit")
-- 字段最长
local index_field_len_limit = index_zset_field_len_limit[1]
-- 索引ID长度
local index_id_len_limit = index_zset_field_len_limit[2]
-- 单个索引最长支持rowid个数
local index_rowid_len_limit = index_zset_field_len_limit[3]

local ifformat = function(a)
    if a == nil then return false end
    if type(a) == "table" and next(a) == nil then
        return false
    end
    if type(a) == "string" and  a == "" then
        return false
    end
    if type(a) == "number" and a == 0 then
        return false
    end
    return a
end

local sql = cjson.decode(ARGV[1])
local explain = ifformat(sql["explain"])
local conditions = sql["where"]
if not conditions or conditions == "" or next(conditions) == nil then
    return "conditions is empty"
end

-- 获取配置
local cache_key = KEYS[1] .. "." .. KEYS[2] .. ".config"
local config = redis.call('get', cache_key)
if config then
    config = cjson.decode(config)
else
    return "not config file"
end

-- 影响索引键
local used_index_key = {}
-- 影响结果集数
local affected_rows = 0
-- rowid打印个数
local print_affected_rowid = 100

-- 获取表字段
local row_key = db_key .. "." .. table_key .. ".row"
local table_fields = redis.call("hget", row_key, "0")
local field_type = redis.call("hget", row_key, "-1")
if not table_fields then
    return "not table_fields or not field_type"
end
table_fields = cjson.decode(table_fields)
-- 字段对应下标
local table_field_cursor = {}
for i,f in pairs(table_fields) do
    table_field_cursor[f] = i
end

-- 字段类型
field_type = cjson.decode(field_type)

-- 获取字段值
local get_value_by_field = function(row, field)
    return row[table_field_cursor[field]]
end

-- 检查条件字段是否存在，纠正条件字段类型
local reset_conditions = function(f, fv)
    if f == 'id' then return fv end
    local n_type = false
    if not table_field_cursor[f] then
        return {"where field [" .. f .. "] is not exist"}
    end
    if field_type[table_field_cursor[f]] == "number" then n_type = true end
    if fv["start"] or fv["end"] then
        if fv["start"] then
            if n_type then fv["start"] = tonumber(fv["start"]) else fv["start"] = tostring(fv["start"]) end
        end
        if fv["end"] then
            if n_type then fv["end"] = tonumber(fv["end"]) else fv["end"] = tostring(fv["end"]) end
        end
    else
        -- 设置为table
        if type(fv) ~= "table" then
            local fValues = {}
            if n_type then fv = tonumber(fv) else fv = tostring(fv) end
            table.insert(fValues, fv)
            fv = fValues
        end
        for i in pairs(fv) do
            if n_type then fv[i] = tonumber(fv[i]) else fv[i] = tostring(fv[i]) end
        end
    end
    return fv
end
for f in pairs(conditions) do
    if f == "or" then
        for fi in pairs(conditions["or"]) do
            conditions["or"][fi] = reset_conditions(fi, conditions["or"][fi])
        end
    else
        conditions[f] = reset_conditions(f, conditions[f])
    end
end

-- 分割字符串
local split = function(str, delimiter)
    local rt= {}
    string.gsub(str, '[^'..delimiter..']+', function(w) table.insert(rt, w) end )
    return rt
end

-- 索引字段长度不足补0，用于范围查找
local strpatch = function(str, patch, length, position)
    if patch == nil then
        patch = "0"
    end
    if length == nil then
        length = index_field_len_limit
    end
    if position == nil then
        -- 默认左补
        position = "left"
    end
    local sl = string.len(str)
    if sl < length then
        for i=1,length-sl do
            if position == "left" then
                str = patch .. str
            else
                str = str .. patch
            end
        end
    end
    return tostring(str)
end

-- bitmap 查询
-- 如果fieldValue为table 则 for in，否则单值查询
local querybybitmap = function(indexField, fieldValues)
    local result, bitresultkey = {}, "bitresult." .. indexField .. "." .. math.random(1,1000000)
    local rowidstr = ""
    if #fieldValues > 1 then
        local bitopkeys = {"bitop", "or", bitresultkey}
        for i in pairs(fieldValues) do
            table.insert(bitopkeys, db_key .. "." .. table_key .. ".bitmap." .. indexField .. "." .. fieldValues[i])
        end
        -- 求并集
        redis.call(unpack(bitopkeys))
        -- 删除索引,不支持delete命令
        for i in pairs(fieldValues) do
            redis.call("expire", db_key .. "." .. table_key .. ".bitmap." .. indexField .. "." .. fieldValues[i], 1)
        end
    else
        bitresultkey = db_key .. "." .. table_key .. ".bitmap." .. indexField .. "." .. fieldValues[1]
    end
    rowidstr = redis.call("get", bitresultkey)
    -- 删除并集索引,不支持delete命令
    redis.call("expire", bitresultkey, 300)

    -- 个数
    local bitcount = redis.call("bitcount", bitresultkey)
    local counti = 0
    local used_index_bitmap_key = db_key .. "." .. table_key .. ".bitmap." .. indexField .. ".".."[" .. table.concat(fieldValues, ',') .. "]"
    if explain then used_index_key[used_index_bitmap_key] = "affected_rows[" .. bitcount .. "] " end

    -- 转化为rowid
    if rowidstr then
        local rowidint, rowid, rowinti = 0, 0, 0
        -- 每个字节从左开始
        for i=1,#rowidstr do
            rowidint = string.byte(rowidstr, i)
            -- 每位从右边开始
            for j=7,0,-1 do
                rowinti = 2^j
                if rowidint >= rowinti then
                    -- 此处标记为rowid
                    rowid = 8*i-j-1
                    result[rowid] = 1
                    rowidint = rowidint - rowinti
                    if explain and counti < print_affected_rowid then
                        used_index_key[used_index_bitmap_key] = used_index_key[used_index_bitmap_key] .. ", " .. rowid
                        counti = counti + 1
                    end
                end
            end
        end
    end

    return result
end

-- 区间查询：普通索引zset查询
local querybyrange = function(indexField, startValue, endValue)
    local result = {}
    -- range，只适用普通索引，其他索引需外部转化为in 列表条件
    local index_key = db_key .. "." .. table_key .. ".index." .. indexField

    -- 获取满足条件的rowid
    local start_value, end_value = "", ""
    if startValue == nil then start_value = "-" else start_value = "[" .. strpatch(startValue) .. ":" end
    if endValue == nil then end_value = "-" else end_value = "[" .. strpatch(endValue) .. ";" end
    local rangeresult = redis.call("zrangebylex", index_key, start_value, end_value)
    -- 删除索引
    redis.call("ZREMRANGEBYLEX", index_key, start_value, end_value)
    local counti, print_rowidstr = 0, ""
    if rangeresult then
        local rowidstr = ""
        for i in pairs(rangeresult) do
            rowidstr = string.sub(rangeresult[i], index_field_len_limit + index_id_len_limit + 3)
            rowidstr = split(rowidstr, ",")
            for j in pairs(rowidstr) do
                result[tonumber(rowidstr[j])]=1
                counti = counti + 1
                if explain and counti < print_affected_rowid then
                    print_rowidstr = print_rowidstr .. "," .. rowidstr[j]
                end
            end
        end
    end
    used_index_key[index_key..">"..start_value .. "," .. end_value] = "affected_rows[" .. counti .. "] " .. print_rowidstr
    return result
end

-- hash查询
local querybyhash = function(indexField, fieldValues)
    local result = {}
    local rowidstr = ""
    local index_key = db_key .. "." .. table_key .. ".hash." .. indexField
    local counti, print_rowidstr = 0, ""
    for i in pairs(fieldValues) do
        rowidstr = redis.call("hget", index_key, fieldValues[i])
        -- 删除索引
        redis.call("hdel", index_key, fieldValues[i])
        if not rowidstr then break end
        rowidstr = split(rowidstr, ",")
        for j in pairs(rowidstr) do
            result[tonumber(rowidstr[j])]=1
            counti = counti + 1
            if explain and counti < print_affected_rowid then
                print_rowidstr = print_rowidstr .. "," .. rowidstr[j]
            end
        end
    end
    used_index_key[index_key..">".."[" .. table.concat(fieldValues, ',') .. "]"] = "affected_rows[" .. counti .. "] " .. print_rowidstr
    return result
end

-- 普通索引查询：zset
local querybyindex = function(indexField, fieldValues)
    local result = {}
    local rowidstr = ""
    local index_key = db_key .. "." .. table_key .. ".index." .. indexField
    local counti, print_rowidstr = 0, ""
    for i in pairs(fieldValues) do
        local start_value = strpatch(fieldValues[i])
        local resultrowid = redis.call("zrangebylex", index_key, "[" .. start_value .. ":", "[" .. start_value .. ";")
        -- 删除索引
        redis.call("ZREMRANGEBYLEX", index_key, "[" .. start_value .. ":", "[" .. start_value .. ";")
        if resultrowid then
            local sublen = index_field_len_limit + index_id_len_limit + 3
            for i in pairs(resultrowid) do
                rowidstr =  string.sub(resultrowid[i], sublen)
                if not rowidstr then break end
                rowidstr = split(rowidstr, ",")
                for j in pairs(rowidstr) do
                    result[tonumber(rowidstr[j])]=1
                    counti = counti + 1
                    if explain and counti < print_affected_rowid then
                        print_rowidstr = print_rowidstr .. "," .. rowidstr[j]
                    end
                end
            end
        end
    end
    used_index_key[index_key..">".."[" .. table.concat(fieldValues, ',') .. "]"] = "affected_rows[" .. counti .. "] " .. print_rowidstr
    return result
end


-- 查询优化器
-- 如果有配置字段的默认引擎, 则直接使用，否则查找引擎类型，找不到则跳过，在最后的记录结果集过滤
local queryengine = function(indexField)
    local indexEngine = ""
    if config["index"][indexField] then
        indexEngine = config["index"][indexField]
    else
        -- hash
        local hash_key = db_key .. "." .. table_key .. ".hash." .. indexField
        local hash_index = redis.call("exists", hash_key)
        if hash_index > 0 then
            indexEngine = "hash"
        else
            -- index
            local index_key = db_key .. "." .. table_key .. ".index." .. indexField
            local index_index = redis.call("exists", index_key)
            if index_index > 0 then
                indexEngine = "index"
            else
                -- bitmap
                local bitmap_key = db_key .. "." .. table_key .. ".bitmap." .. indexField .. ".*"
                local bitmap_index = redis.call("keys", bitmap_key)
                if bitmap_index and next(bitmap_index) ~= nil then
                    indexEngine = "bitmap"
                end
            end
        end
    end
    return indexEngine
end

-- 查询解析器
local queryrowids = function(indexField, fieldValue, indexEngine)
    local result = {}
    -- 区间查找只支持zset普通索引
    if  fieldValue["start"] or fieldValue["end"] then
        if indexEngine ~= "index" then return result end
        result = querybyrange(indexField, fieldValue["start"], fieldValue["end"])
    else
        if indexEngine == 'bitmap' then
            result = querybybitmap(indexField, fieldValue)
        else

            if indexEngine == 'hash' then
                result = querybyhash(indexField, fieldValue)
            else
                -- 默认索引引擎
                result = querybyindex(indexField, fieldValue)
            end
        end
    end
    return result
end

-- 查询结果集ID rowids,仅支持单索引
local rowids = {}
for indexField in pairs(conditions) do
    local fieldValue = conditions[indexField]
    -- 如果条件为rowid，则直接查找hset记录集
    if indexField == "id" then
        if type(fieldValue) == "string" then
            rowids[fieldValue] = 1
        else
            for i in pairs(fieldValue) do
                rowids[fieldValue[i]] = 1
            end
        end
    else
        local indexEngine = queryengine(indexField)
        -- 获取rowids 并删除条件索引
        rowids = queryrowids(indexField, fieldValue, indexEngine)
    end

    break
end



-- rowid反向删除row索引，先查找出row，对应索引引擎键
-- 整合索引，如：index=>v_day=>rowid，bitmap=>channel_id=>rowid, hash=>adid=>rowid
local row_key = db_key .. "." .. table_key .. ".row"
local row, row_index = {}, {}
local indexEngine, indexFields = "", {}
for i in pairs(table_fields) do
   local field = table_fields[i]
   -- 排除主键和条件索引
   if field ~= "id" and not conditions[field]  then
       indexEngine = queryengine(field)
       if indexEngine ~= "" then
           indexFields[field] = indexEngine
           row_index[field] = {}
       end
   end
end

-- rangeid 区间范围键
local rowid_key = function(rowid)
    return  math.ceil(rowid/row_page_range)
end

local row_len_str = string.len(row_len_limit) -- 记录长度的字符串格式长度 1000 => 4
local rowid_len_str = string.len(row_page_range) - 1 -- rowid的字符串格式长度 1000 => 4-1
-- rowid 偏移量
local rowid_offset = function(rowid, rowid_k)
    rowid = rowid - (rowid_k-1)*row_page_range
    -- 补0
    return strpatch(rowid, "0", rowid_len_str)
end

-- 影响结果集
local affected_rows = 0
local affected_krows = 0
-- rowid区间集合
local krowids = {}
for rowid in pairs(rowids) do
    local rowid_k = rowid_key(rowid)
    if not krowids[rowid_k] then
        krowids[rowid_k] = {}
        affected_krows = affected_krows + 1
    end
    local rowid_os = rowid_offset(rowid, rowid_k)
    krowids[rowid_k][rowid_os] = rowid
    affected_rows = affected_rows + 1
end

-- 删除结果集 hdel
-- 计算rowid 集区间，找出对应记录集, 删除并更新
for rowid_k,rowids in pairs(krowids) do
    local rowpage = redis.call("hget", row_key, rowid_k)
    local newrowpage = ""
    if rowpage then
        -- 遍历rowpage全字符串,取出旧记录
        local rowpage_len = string.len(rowpage)
        local block_len, rowid_os, row_len, block_start = 0, 0, 0, 1
        while block_start < rowpage_len do
            rowid_os = string.sub(rowpage, block_start+1, block_start+rowid_len_str)
            row_len = tonumber(string.sub(rowpage, block_start+rowid_len_str+2, block_start+rowid_len_str+1+row_len_str))
            block_len = 1+rowid_len_str+1+row_len_str+row_len
            if not rowids[rowid_os] then
                newrowpage = newrowpage .. string.sub(rowpage, block_start, block_start+block_len-1)
            else
                -- 找到要删除的记录集，并获取关联的二级索引
                row = string.sub(rowpage, block_start+rowid_len_str+1+row_len_str+1, block_start+block_len-1)
                row = cjson.decode(row)
                for indexField, indexEngine in pairs(indexFields) do
                    local fieldValue = get_value_by_field(row, indexField)
                    if not row_index[indexField][fieldValue] then row_index[indexField][fieldValue] = {} end
                    row_index[indexField][fieldValue][rowids[rowid_os]] = 1
                end
            end
            block_start = block_start+block_len
        end

    end
    if newrowpage ~= "" then
        redis.call("hset", row_key, rowid_k, newrowpage)
    else
        redis.call("hdel", row_key, rowid_k)
    end
end

krowids = nil


-- 删除关联二级索引
for indexField in pairs(row_index) do
    if indexFields[indexField] == 'bitmap' then
        -- bitmap
        local counti, print_rowidstr,fieldValues, setbitres = 0, "", "",0
        for fieldValue,idValue in pairs(row_index[indexField]) do
            local index_key = db_key .. "." .. table_key .. ".bitmap." .. indexField .. "." .. fieldValue
            fieldValues = fieldValues .. "," .. fieldValue
            for rowid in pairs(idValue) do
                setbitres = redis.call("setbit", index_key, rowid, 0)
                if setbitres > 0 then
                    counti = counti + 1
                    if explain and counti <= print_affected_rowid then
                        print_rowidstr = print_rowidstr .. "," .. rowid
                    end
                end

            end

        end
        if explain then
            local used_index_bitmap_key = db_key .. "." .. table_key .. ".bitmap." .. indexField .. ".".."[" .. fieldValues .. "]"
            used_index_key[used_index_bitmap_key] = "ref affected_rows[" .. counti .. "] " .. print_rowidstr
        end
    else
        if indexFields[indexField] == 'hash' then
            -- hash
            local counti, print_rowidstr, fieldValues = 0, "", ""
            local rowidstr, newrowidstr = "", ""
            local index_key = db_key .. "." .. table_key .. ".hash." .. indexField
            for fieldValue,idValue in pairs(row_index[indexField]) do
                fieldValues = fieldValues .. "," .. fieldValue
                rowidstr = redis.call("hget", index_key, fieldValue)
                if rowidstr then
                    rowidstr = split(rowidstr, ",")
                    for ri, rowid in pairs(rowidstr) do
                        if not idValue[tonumber(rowid)] then
                            newrowidstr = newrowidstr .. "," .. rowid
                        else
                            counti = counti + 1
                            if explain and counti <= print_affected_rowid then
                                print_rowidstr = print_rowidstr .. "," .. rowid
                            end
                        end
                    end
                    if newrowidstr ~= "" then
                        newrowidstr = string.sub(newrowidstr, 2)
                        -- 更新
                        redis.call('hset', index_key, newrowidstr)
                    else
                        -- 为空，删除
                        redis.call("hdel", index_key, fieldValue)
                    end
                end

            end
            used_index_key[index_key..">".."[" .. fieldValues .. "]"] = "ref affected_rows[" .. counti .. "] " .. print_rowidstr
        else
            -- zset, 区间删除
            local counti, print_rowidstr, fieldValues = 0, "", ""
            local index_key = db_key .. "." .. table_key .. ".index." .. indexField
            for fieldValue,idValue in pairs(row_index[indexField]) do
                local rowidstr, newrowidstr = "", ""
                local sublen = index_field_len_limit + index_id_len_limit + 3
                local fieldValue = strpatch(fieldValue)
                fieldValues = fieldValues .. "," .. fieldValue
                local resultrowid = redis.call("zrangebylex", index_key, "[" .. fieldValue .. ":", "[" .. fieldValue .. ";")
                -- 直接删除旧索引，等待重新分配
                redis.call("zremrangebylex", index_key, "[" .. fieldValue .. ":", "[" .. fieldValue .. ";")
                if resultrowid then
                    -- 删除rowid后，合并所有rowid，重新分配indexid, 整理碎片减少空间占用
                    for i in pairs(resultrowid) do
                        rowidstr =  string.sub(resultrowid[i], sublen)
                        rowidstr = split(rowidstr, ",")
                        for ri, rowid in pairs(rowidstr) do
                            if not idValue[tonumber(rowid)] then
                                newrowidstr = newrowidstr .. "," .. rowid
                            else
                                counti = counti + 1
                                if explain and counti <= print_affected_rowid then
                                    print_rowidstr = print_rowidstr .. "," .. rowid
                                end
                            end
                        end

                    end
                    -- 不为空则重新分配
                    if newrowidstr ~= "" then
                        newrowidstr = string.sub(newrowidstr, 2)
                        -- 删除旧indexid
                        redis.call("hdel", index_key .. ".indexid", fieldValue)
                        local indexid = tonumber(strpatch("1", "0", index_id_len_limit, "right"))
                        local last_index_value_key = fieldValue .. ":" .. indexid .. ":"
                        local countj, substart = 0, 1
                        local rowidlen = string.len(newrowidstr)
                        -- 每100个rowid更新一次
                        for j=1,rowidlen do
                            -- 更新rowid
                            if string.sub(newrowidstr, j, j) == "," then
                                countj = countj + 1
                                if countj >= index_rowid_len_limit then
                                    redis.call('zadd', index_key, 0, last_index_value_key .. string.sub(newrowidstr, substart, j-1))
                                    countj, substart  = 0, j+1
                                    indexid = indexid + 1
                                    last_index_value_key = fieldValue .. ":" .. indexid .. ":"
                                end
                            end

                        end
                        if countj > 0 then
                            redis.call('zadd', index_key, 0, last_index_value_key .. string.sub(newrowidstr, substart))
                        end
                        -- 更新新indexid
                        redis.call("hset", index_key .. ".indexid", fieldValue, indexid)
                    end
                end
            end
            used_index_key[index_key..">".."[" .. fieldValues .. "]"] = "ref affected_rows[" .. counti .. "] " .. print_rowidstr
        end
    end
end



if explain then
    -- rowids
    for uk,uv in pairs(used_index_key) do
        redis.log(redis.LOG_NOTICE, "used_index: " .. uk .. " " .. uv)
    end
    local print_rowidstr = ""
    local counti = 0
    for rowid in pairs(rowids) do
        print_rowidstr = print_rowidstr .. "," .. rowid
        counti = counti + 1
        if counti >= print_affected_rowid then break end
    end
    redis.log(redis.LOG_NOTICE, "All affected_rows: [" .. affected_rows .. "] " .. " affected_krows: [" .. affected_krows .. "] " .. print_rowidstr)
end
used_index_key = nil



return true
