-- 根据索引查询记录，支持多索引查询
-- 1.索引引擎 bitmap hash index(zset)
-- 2.索引类型 in range ，其中range仅支持zset普通索引
-- 3.查找rowid，求各个索引条件的并集或交集
-- 4.根据rowids查询对应哈希表记录, hmget支持批量查询,1w rowids秒查
-- 5.根据结果集，全文过滤条件
-- 注意：直接rowid查询不走索引

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

local select_sql = ifformat(sql["select"])
if select_sql then
    if not select_sql["sum"] and not select_sql["count"] and not select_sql["distinct"] then
        return "select err: not in {sum, count, distinct}"
    end
end

local groupby = ifformat(sql["groupby"])
local orderby = ifformat(sql["orderby"])
local limit = ifformat(sql["limit"])
local explain = ifformat(sql["explain"])
local conditions = ifformat(sql["where"])

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
if not table_fields or not field_type then
    return "not table_fields or not field_type"
end
table_fields = cjson.decode(table_fields)

-- 字段类型
field_type = cjson.decode(field_type)

-- 字段对应下标
local table_field_cursor = {}
for i,f in pairs(table_fields) do
    table_field_cursor[f] = i
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
if conditions then
    for f in pairs(conditions) do
        if f == "or" then
            for fi in pairs(conditions["or"]) do
                conditions["or"][fi] = reset_conditions(fi, conditions["or"][fi])
            end
        else
            conditions[f] = reset_conditions(f, conditions[f])
        end
    end
end

-- 分割字符串
local split = function(str, delimiter)
    local rt= {}
    string.gsub(str, '[^'..delimiter..']+', function(w) table.insert(rt, w) end )
    return rt
end

-- 分隔符为字符串的分割方式
local splits = function (str, delimiter)
    if str==nil or str=='' or delimiter==nil then
        return nil
    end

    local result = {}
    for match in (str..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match)
    end
    return result
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

-- 获取运算字段 +-*/,，在结果集后单独计算
local cacl_field = {}
for i in pairs(select_sql) do
    if type(select_sql[i]) == "string" then
        local slf = splits(select_sql[i], " as ")
        local slf_field, slf_alias = "", ""
        for j,c in pairs({'%+', '%-', '%*', '%/', '%,'}) do
            if string.find(select_sql[i], c) then
                if slf[2] then slf_alias = slf[2] else slf_alias = string.gsub(slf[1], c, '_') end
                slf_field = slf[1]
                -- {field1, field2, c}
                cacl_field[slf_alias] = split(slf_field, string.sub(c, 2))
                table.insert(cacl_field[slf_alias], string.sub(c, 2))
                -- 从普通select字段中移除运算字段
                table.remove(select_sql, i)
            end
        end
    end
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
        -- 删除并集,不支持delete命令
        redis.call("expire", bitresultkey, 300)
    else
        bitresultkey = db_key .. "." .. table_key .. ".bitmap." .. indexField .. "." .. fieldValues[1]
    end
    rowidstr = redis.call("get", bitresultkey)
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
                    if explain and counti < print_affected_rowid then
                        used_index_key[used_index_bitmap_key] = used_index_key[used_index_bitmap_key] .. ", " .. rowid
                        counti = counti + 1
                    end
                    rowidint = rowidint - rowinti
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
    if fieldValue["start"] or fieldValue["end"] then
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

-- filter 过滤器
local filter = {}

-- 查询结果集ID rowids
local get_rowids = function()
    local rowids = {}
    for indexField in pairs(conditions) do

        rowids[indexField] = {}
        -- 如果条件为rowid，则直接查找hset记录集
        if indexField == "id" then
            if type(conditions[indexField]) == 'string' then conditions[indexField] = {conditions[indexField]} end
            for i,_id in pairs(conditions[indexField]) do
                rowids["id"][_id] = 1
            end
            break
        end

        if indexField == "or" then
            -- or
            rowids["or"] = {}
            for k in pairs(conditions["or"]) do
                -- 找不到引擎则跳过，在最后的记录结果集过滤
                local indexEngine = queryengine(k)
                if indexEngine == "" then
                    if not filter["or"] then filter["or"] = {} end
                    filter["or"][k] = conditions["or"][k]
                else
                    -- range in
                    local rowidsorres = queryrowids(k, conditions["or"][k], indexEngine)
                    -- 求并集
                    for i in pairs(rowidsorres) do
                        rowids["or"][i] = 1
                    end
                end
            end
        else
            -- 找不到引擎则跳过，在最后的记录结果集过滤
            local indexEngine = queryengine(indexField)
            if indexEngine == "" then
                filter[indexField] = conditions[indexField]
            else
                rowids[indexField] = queryrowids(indexField, conditions[indexField], indexEngine)
            end
        end

    end
    return rowids
end


-- order by func
local partion = function(array,left,right,compareFunc) return 0 end
local quick = function(array,left,right,compareFunc) end

partion = function (array,left,right,compareFunc)
    local key = array[left] -- 哨兵  一趟排序的比较基准
    local index = left
    array[index],array[right] = array[right],array[index] -- 与最后一个元素交换
    local i = left
    while i< right do
        if compareFunc( key,array[i]) then
            array[index],array[i] = array[i],array[index]-- 发现不符合规则 进行交换
            index = index + 1
        end
        i = i + 1
    end
    array[right],array[index] = array[index],array[right] -- 把哨兵放回
    return index;
end

quick = function (array,left,right,compareFunc)
    if(left < right ) then
        local index = partion(array,left,right,compareFunc)
        quick(array,left,index-1,compareFunc)
        quick(array,index+1,right,compareFunc)
    end
end

local quickSort = function (array,compareFunc)
    quick(array,1,#(array),compareFunc)
end


-- 是否id limit or orderby
local orderbyid = false;
local limitid = false;
-- 返回结果集个数
local rows_number = 0

-- 求交集rowids
local get_urowids = function()
    local rowids = get_rowids()
    -- 取出其中一个集合，对其他集合进行交集
    local next_Field, next_rowids = next(rowids)
    if next(rowids) == nil or not next_rowids then return {} end
    local urowids, u, ufields = {}, true, {}
    for indexField in pairs(conditions) do
        -- 除掉非索引集 filter
        if (indexField ~= next_Field and not filter[indexField]) or indexField == "or" or indexField == "id" then
            table.insert(ufields, indexField)
        end
    end
    for rowid in pairs(next_rowids) do
        u = true
        for i in pairs(ufields) do
            if not rowids[ufields[i]][rowid] then u = false; break end
        end
        if u then
            table.insert(urowids, rowid)
        end
    end

    -- 没有groupby orderby 或者orderby id 则 limit
    if not select_sql then
        if orderby and orderby["desc"] and orderby["desc"][1] == "id"then
            local orderbyfunc = function(x, y) return x<y end
            quickSort(urowids, orderbyfunc)
            orderbyid = true
        elseif orderby and orderby["asc"] and orderby["asc"][1] == "id" then
            local orderbyfunc = function(x, y) return x>y end
            quickSort(urowids, orderbyfunc)
            orderbyid = true
        end

        if limit then
            local limit_rows = {}
            for i=limit[1],limit[2] do
                table.insert(limit_rows, urowids[i])
            end
            rows_number = #urowids
            urowids = limit_rows
            limitid = true
        end

    end
    return urowids
end

-- 获取字段值
local get_value_by_field = function(row, field)
    return row[table_field_cursor[field]]
end

-- 过滤器
local filter_status = function(f, v, filter2)
    local fstatus = false
    -- 区间过滤
    if filter2[f]["start"] or filter2[f]["end"] then
        if filter2[f]["start"] and filter2[f]["end"] then
            if filter2[f]["start"] <= v and v <= filter2[f]["end"] then
                fstatus = true
            end
        else
            if (filter2[f]["start"] and filter2[f]["start"] <= v ) or (filter2[f]["end"] and v <= filter2[f]["end"]) then
                fstatus = true
            end
        end
    else
        for fi in pairs(filter2[f]) do
            if v == filter2[f][fi] then fstatus = true; break end
        end
    end
    return fstatus
end
local filter_row = function(row)
    local fstatus = true
    for f in pairs(filter) do
        if f == "or" then
            for fi in pairs(filter["or"]) do
                fstatus = filter_status(fi, get_value_by_field(row, fi), filter["or"])
                if fstatus then break end
            end
        else
            fstatus = filter_status(f, get_value_by_field(row, f), filter)
            if not fstatus then break end
        end
    end
    return fstatus
end


-- rangeid 区间范围键
local rowid_key = function(rowid)
    return  math.ceil(rowid/row_page_range)
end

local row_len_str = string.len(row_len_limit) -- 记录长度的字符串格式长度 1000 => 4
local rowid_len_str = string.len(row_page_range) - 1 -- rowid的字符串格式长度 1000 => 4-1
-- rowid=>偏移量
local rowid_offset = function(rowid, rowid_k)
    rowid = rowid - (rowid_k-1)*row_page_range
    -- 补0
    return strpatch(rowid, "0", rowid_len_str)
end
-- 偏移量=>rowid
local rowoffset_id = function(rowid_os, rowid_k)
    return rowid_os + (rowid_k-1)*row_page_range
end

-- 查询rowid对应的记录row
-- 影响到的页数，结果集数, 打印ID
local affected_pages, affected_rows, print_rowid = 0, 0, {}

-- rowid区间page集合
local get_krowids = function()

    -- 二级缓存
    local cache_key_2 = ifformat(sql["cache_key_2"])
    local cache_time_2 = 30
    if ifformat(sql["cache_time_2"]) then cache_time_2 = tonumber(sql["cache_time_2"]) end
    if not explain and cache_key_2 then
        local krowids = redis.call("get", cache_key_2)
        --redis.log(redis.LOG_NOTICE, 'krowids-CACHE: '..krowids)
        if krowids then
            krowids = cjson.decode(krowids)
            return krowids
        end
    end
    local krowids = {}
    if conditions then
        local urowids = get_urowids()
        if not ifformat(urowids) then return {} end
        for j,rowid in pairs(urowids) do
            local rowid_k = tostring(rowid_key(rowid))
            if not krowids[rowid_k] then
                krowids[rowid_k] = {}
            end
            local rowid_os = rowid_offset(rowid, rowid_k)
            krowids[rowid_k][rowid_os] = rowid
        end
        urowids = nil
    else
        -- 全文查找
        local row_keys = redis.call("hkeys", row_key)
        if not ifformat(row_keys) then return "full query rowpage keys empty" end
        -- 去除头部字段和类型key -1,0
        for j,rowid_k in pairs(row_keys) do
            if tonumber(rowid_k) > 0 then
                krowids[tostring(rowid_k)] = {}
            end
        end
    end
    -- 缓存30s
    if not explain and cache_key_2 then
        redis.call("set", cache_key_2, cjson.encode(krowids))
        redis.call('expire', cache_key_2, cache_time_2)
    end
    return krowids
end


-- 聚合器 get rows by select
local select_rows = function()
    local rows = {}
    -- select func
    local isdistinct, distinctfield = false, ""
    local select_fields = {}
    local group_rows = {}
    -- select 分为统计字段和分组字段
    local select_field_group = {}
    for s in pairs(select_sql) do
        -- distinct 与 sum count不可共用
        if s == "distinct" then
            isdistinct = true
            distinctfield = select_sql[s][1]
            break
        end

        if type(select_sql[s]) == 'string' then
            -- 分组字段
            table.insert(select_field_group, select_sql[s])
        else
            -- 统计字段
            for sf in pairs(select_sql[s]) do
                -- slf[1] 字段 slf[2] 字段别名
                local slf = splits(select_sql[s][sf], " as ")
                local slf_field, slf_alias = "", ""
                if slf[2] then slf_alias = slf[2] else slf_alias = s .. "." .. slf[1] end
                slf_field = slf[1]

                if s == "sum" then
                    -- 求和
                    if not select_fields[slf_field] then select_fields[slf_field] = {} end
                    select_fields[slf_field][slf_alias] = function(group_key, slf_field, slf_alias, appendValue)
                        if not group_rows[group_key][slf_alias] then group_rows[group_key][slf_alias] = 0 end
                        group_rows[group_key][slf_alias] = group_rows[group_key][slf_alias] + appendValue
                    end

                else
                    if s == "count" then
                        -- 计数
                        if string.sub(slf[1], 1, 8) == "distinct" then
                            local slf_field = string.sub(slf[1], 10)
                            if not select_fields[slf_field] then select_fields[slf_field] = {} end
                            select_fields[slf_field][slf_alias] = function(group_key, slf_field, slf_alias, appendValue)
                                if not group_rows[group_key][slf_alias] then group_rows[group_key][slf_alias] = {} end
                                group_rows[group_key][slf_alias][appendValue] = 1
                            end
                        else
                            if not select_fields[slf_field] then select_fields[slf_field] = {} end
                            select_fields[slf_field][slf_alias] = function(group_key, slf_field, slf_alias, appendValue)
                                if not group_rows[group_key][slf_alias] then group_rows[group_key][slf_alias] = 0 end
                                group_rows[group_key][slf_alias] = group_rows[group_key][slf_alias] + 1
                            end
                        end
                    end
                end
            end
        end
    end

    -- groupby func
    local groupby_func = function(row)
        local group_key = "all"
        if not group_rows[group_key] then
            group_rows[group_key] = {}
        end
        return group_key
    end
    if groupby then
        groupby_func = function(row)
            local group_key = ""
            for g in pairs(groupby) do
                group_key = group_key .. "," .. get_value_by_field(row, groupby[g])
            end
            if not group_rows[group_key] then
                group_rows[group_key] = {}
                -- 普通字段 groupby 字段同组
                for i in pairs(select_field_group) do
                    group_rows[group_key][select_field_group[i]] = get_value_by_field(row, select_field_group[i])
                end
                --group_rows[group_key][groupby[g]] = get_value_by_field(row, groupby[g])
            end
            return group_key
        end
    end
    local krowids = get_krowids()
    local counti = 0
    for rowid_k,rowids in pairs(krowids) do
        affected_pages = affected_pages + 1
        local rowpage = redis.call("hget", row_key, rowid_k)
        if rowpage then
            -- 遍历rowpage全字符串
            local rowpage_len = string.len(rowpage)
            local block_len, rowid_os, row_len, block_start = 0, 0, 0, 1
            local row = ""
            while block_start < rowpage_len do
                rowid_os = string.sub(rowpage, block_start+1, block_start+rowid_len_str)
                row_len = tonumber(string.sub(rowpage, block_start+rowid_len_str+2, block_start+rowid_len_str+1+row_len_str))
                block_len = 1+rowid_len_str+1+row_len_str+row_len
                if not conditions or rowids[rowid_os] then
                    row = string.sub(rowpage, block_start+rowid_len_str+1+row_len_str+1, block_start+block_len-1)
                    row = cjson.decode(row)
                    if row and filter_row(row) then
                        affected_rows = affected_rows + 1
                        counti = counti + 1
                        if explain and counti < print_affected_rowid then
                            table.insert(print_rowid, rowoffset_id(rowid_os, rowid_k))
                        end
                        -- select sum count distinct
                        local group_key = groupby_func(row)
                        -- distinct 与 sum count groupby不可共用
                        if isdistinct then
                            group_rows["all"]["distinct"] = {}
                            group_rows["all"]["distinct"][get_value_by_field(row, select_sql["distinct"])] = 1
                        else
                            for slf_field in pairs(select_fields) do
                                local appendValue = get_value_by_field(row, slf_field)
                                if appendValue ~= nil then
                                    for slf_alias in pairs(select_fields[slf_field]) do
                                        select_fields[slf_field][slf_alias](group_key, slf_field, slf_alias, appendValue)
                                    end
                                end
                            end
                        end
                    end

                end
                -- 初始化block开始位置
                block_start = block_start+block_len
            end

        end

    end
    if next(group_rows) == nil then return {} end
    if isdistinct then
        local row = {}
        -- 字段头
        table.insert(rows, {distinctfield})
        for v in pairs(group_rows["all"]["distinct"]) do
            table.insert(rows, {v})
        end
    else
        -- 字段头
        local return_row_header = {}
        local group_key,group_row = next(group_rows)
        for slf in pairs(group_row) do
            table.insert(return_row_header, slf)
        end
        table.sort(return_row_header)
        table.insert(rows, return_row_header)

        for group_key,group_row in pairs(group_rows) do
            local row = {}
            for i,slf in pairs(return_row_header) do
                if type(group_row[slf]) == "table" then
                    local countdistinct = 0
                    for field in pairs(group_row[slf]) do
                        countdistinct = countdistinct + 1
                    end
                    table.insert(row, countdistinct)
                else
                    table.insert(row, group_row[slf])
                end
            end
            table.insert(rows, row)
        end
    end
    return rows
end

-- 聚合器 get rows
local get_rows = function()
    -- 字段头
    local rows = {}
    table.insert(table_fields, "id")
    table.insert(rows, table_fields)
    local counti = 0
    local krowids = get_krowids()
    for rowid_k,rowids in pairs(krowids) do
        affected_pages = affected_pages + 1
        local rowpage = redis.call("hget", row_key, rowid_k)
        if rowpage then
            -- 遍历rowpage全字符串
            local rowpage_len = string.len(rowpage)
            local block_len, rowid_os, row_len, block_start = 0, 0, 0, 1
            local row = ""
            while block_start < rowpage_len do
                rowid_os = string.sub(rowpage, block_start+1, block_start+rowid_len_str)
                row_len = tonumber(string.sub(rowpage, block_start+rowid_len_str+2, block_start+rowid_len_str+1+row_len_str))
                block_len = 1+rowid_len_str+1+row_len_str+row_len
                if not conditions or rowids[rowid_os] then

                    row = string.sub(rowpage, block_start+rowid_len_str+1+row_len_str+1, block_start+block_len-1)
                    row = cjson.decode(row)
                    if row and filter_row(row) then
                        affected_rows = affected_rows + 1
                        counti = counti + 1
                        if explain and counti < print_affected_rowid then
                            table.insert(print_rowid, rowoffset_id(rowid_os, rowid_k))
                        end
                        table.insert(row, rowids[rowid_os])
                        table.insert(rows, row)
                    end
                end
                -- 初始化block开始位置
                block_start = block_start+block_len
            end

        end

    end
    if #rows < 2 then rows = {} end
    return rows
end

-- 结果集
local rows = {}
-- 三级缓存
local cache_key_3 = ifformat(sql["cache_key_3"])
local cache_time_3 = 30
if ifformat(sql["cache_time_3"]) then cache_time_3 = tonumber(sql["cache_time_3"]) end
if not explain and cache_key_3 then
    rows = redis.call("get", cache_key_3)
    if rows then
        rows = cjson.decode(rows)
    end
end

if not ifformat(rows) then
    if not select_sql or select_sql[1] == '*' then
        rows = get_rows()
    else
        rows = select_rows()
    end
    if not explain and cache_key_3 then
        redis.call("set", cache_key_3, cjson.encode(rows))
        redis.call('expire', cache_key_3, cache_time_3)
    end
end

if explain then
    -- rowids
    for uk,uv in pairs(used_index_key) do
        redis.log(redis.LOG_NOTICE, "used_index: " .. uk .. " " .. uv)
    end
    local print_rowidstr = ""
    if affected_rows>0 then
        for ui=1,print_affected_rowid do
            if not print_rowid[ui] then break end
            print_rowidstr = print_rowidstr .. "," .. print_rowid[ui]
        end
    end
    redis.log(redis.LOG_NOTICE, "All affected_rows: [" .. affected_rows .. "] affected_pages: [" .. affected_pages .. "]" .. print_rowidstr)
end

if next(rows) == nil then return {} end

-- order by
local rows_header = rows[1]
local rows_header_cursor = {}
local rows_header_dict = {}
for i,f in pairs(rows_header) do
    rows_header_dict[f] = 1
end


-- 格式化字段
for i,f in pairs(rows_header) do
    local f_start = string.find(f, '%.')
    if f_start and not rows_header_dict[string.sub(f, f_start+1)] then
        rows_header_cursor[string.sub(f, f_start+1)] = i
        rows_header[i] = string.sub(f, f_start+1)
    else
        rows_header_cursor[f] = i
    end

end

local cacl_func = function(row) return true end
-- 加入运算字段 +-*/,, 头部字段及相应的数据
local ifcacl_all = false -- 是否全文计算
if ifformat(cacl_field) then
    for slf_alias, f in pairs(cacl_field) do
        -- 补充字段
        table.insert(rows_header, slf_alias)
        -- 下标
        rows_header_cursor[slf_alias] = #rows_header
        -- 类型
        field_type[#rows_header] = "string"
        -- 是否全文计算, 根据是否orderby 当前字段，判断是否提前计算，或者在limit之后计算
        if orderby and orderby['asc'] then
            for i,of in pairs(orderby['asc']) do
                if of == slf_alias then ifcacl_all = true end
            end
        elseif orderby and orderby['desc'] then
            for i,of in pairs(orderby['desc']) do
                if of == slf_alias then ifcacl_all = true end
            end
        end
    end

    cacl_func = function(row)
        for j=#row+1,#rows_header do
            local slf_alias = rows_header[j]

            local v1 = tonumber(row[rows_header_cursor[cacl_field[slf_alias][1]]])
            if not v1 then v1 = 0 end
            local v2 = tonumber(row[rows_header_cursor[cacl_field[slf_alias][2]]])
            if not v2 then v2 = 0 end
            local v12 = 0

            if cacl_field[slf_alias][3] == '+' then
                v12 = v1 + v2
            elseif cacl_field[slf_alias][3] == '-' then
                v12 = v1 - v2
            elseif cacl_field[slf_alias][3] == '*' then
                v12 = v1 * v2
            elseif cacl_field[slf_alias][3] == '/' then
                if v2 == 0 then
                    v12 =0
                else
                    v12 = math.floor(v1*10000 / v2)/10000
                end
            elseif cacl_field[slf_alias][3] == ',' then
                v12 = v1 .. ',' .. v2
            end
            row[j] = tostring(v12)
        end
    end

end

-- 全文本计算cacl字段
if ifcacl_all or not limit then
    for i,row in pairs(rows) do
        -- 补充数据
        cacl_func(rows[i])
    end
end

-- 移除字段头,用于排序
table.remove(rows, 1)

if orderby and not orderbyid then
    local orderbyfunc = function(x, y) return false end

    if orderby["desc"] then
        if #orderby["desc"] == 1 then
            local vcursor = rows_header_cursor[orderby["desc"][1]]
            orderbyfunc = function(x, y)
                return x[vcursor] < y[vcursor]
            end
        else

            orderbyfunc = function(x, y)
                local ox, oy = "", ""
                for o,v in pairs(orderby["desc"]) do
                    ox = ox .. strpatch(x[rows_header_cursor[v]])
                    oy = oy .. strpatch(y[rows_header_cursor[v]])
                end
                return x[rows_header_cursor[v]] < y[rows_header_cursor[v]]
            end
        end
    else
        if #orderby["asc"] == 1 then
            local vcursor = rows_header_cursor[orderby["asc"][1]]
            orderbyfunc = function(x, y)
                return x[vcursor] > y[vcursor]
            end
        else
            orderbyfunc = function(x, y)
                local ox, oy = "", ""
                for o,v in pairs(orderby["asc"]) do
                    ox = ox .. strpatch(x[rows_header_cursor[v]])
                    oy = oy .. strpatch(y[rows_header_cursor[v]])
                end
                return ox > oy
            end
        end
    end

    quickSort(rows, orderbyfunc)
end

-- count all
if not limitid then
    -- 结果集个数
    rows_number = #rows
end

-- limit
if limit and not limitid then
    local limit_rows = {}
    for i=limit[1],limit[2] do
        if not rows[i] then break end
        if not ifcacl_all then cacl_func(rows[i]) end
        table.insert(limit_rows, rows[i])
    end
    rows = limit_rows
end

-- 字段头、记录、记录数
return {rows_header, rows, rows_number}
