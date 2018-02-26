-- 添加表记录
-- 记录集 hash结构，每1000个rowid组成一个键
-- hash bitmap 索引长度 512M
-- 普通索引 zset 索引长度 10位字段值+6位索引ID+100个rowid  fieldValue:indexid:rowid...
-- hash记录集格式  rowidstart:rowidend => ~rowid1:len1~[row1]~rowid2:len2~[row1]
-- 获取配置
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

local db_key = KEYS[1]
local table_key = KEYS[2]
local rows = cjson.decode(ARGV[1])
local indexFields = ARGV[2]

if not rows or (type(rows) == "table" and next(rows) == nil) then
    return "not rows"
end

-- 调试日志
-- redis.log(redis.LOG_NOTICE, "debug")
-- json bug key为数字并大于10出现的编码bug
--local json=cjson.new()
--json.encode_sparse_array(true, 1)
--redis.log(redis.LOG_NOTICE, "krowids: " .. json.encode(krowids))

-- 分割字符串
local split = function(s, p)
    local rt= {}
    string.gsub(s, '[^'..p..']+', function(w) table.insert(rt, w) end )
    return rt
end

-- 查找字符串出现次数
local countstr = function(str, findstr)
    local scount, si = 0, 1
    local comstr = ""
    local findstrlen = string.len(findstr)
    while true do
        comstr = string.sub(str, si, si+findstrlen-1)
        if string.len(comstr) < findstrlen then
            break
        end
        if comstr == findstr then
            scount = scount + 1
        end
        si = si + 1
    end
    return scount
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

-- 索引字段
local indexFieldsTable = {}
if indexFields then
    indexFields = split(indexFields, ",")
    for i in pairs(indexFields) do
        local indexEngine = "index"
        -- 特殊字段索引，根据config
        if config["index"][indexFields[i]] then
            indexEngine = config["index"][indexFields[i]]
        end
        local indexField = indexFields[i]
        if string.find(indexFields[i], "%.") then
            local indexef = split(indexFields[i], ".")
            indexEngine, indexField = indexef[1], indexef[2]
        end
        indexFieldsTable[indexField] = indexEngine
    end
end


-- 批量添加记录
local row_key = db_key .. "." .. table_key .. ".row"
-- 是否已格式化记录行
local row_data_format = true
-- 压缩记录，去掉字段名，去掉主键ID，表头标识字段 {"field1", "field2", ...}
local row_header = {}
-- 带有字段的字典记录行，未排序
if rows[1]["id"] then row_data_format = false end
local get_value_by_field = function(row, field) return false end
if not row_data_format then
    for k,v in pairs(rows[1]) do
        -- 去掉ID字段
        if k ~= "id" then
            table.insert(row_header, k)
        end
    end

    -- 获取原数据记录行对应字段值
    get_value_by_field = function(row, field)
        return row[field]
    end
else
    -- 格式化后的记录行,已排序的记录
    -- 原数据记录行字段对应下标
    local table_field_cursor = {}
    for i,f in pairs(rows[1]) do
        table_field_cursor[f] = i
    end

    -- 获取原数据记录行对应字段值
    get_value_by_field = function(row, field)
        return row[table_field_cursor[field]]
    end

    for i,k in pairs(rows[1]) do
        -- 去掉ID字段
        if k ~= "id" then
            table.insert(row_header, k)
        end
    end

    -- 首行为字段头，删除字段行
    table.remove(rows, 1)
end

-- 排序
table.sort(row_header)
redis.call("hset", row_key, "0", cjson.encode(row_header))

-- 字段类型 {"number", "string", "table"}
local row_type = {}
for h,v in pairs(row_header) do
    table.insert(row_type, type(get_value_by_field(rows[1], v)))
end
redis.call("hset", row_key, "-1", cjson.encode(row_type))

-- rangeid 区间范围键
local rowid_key = function(rowid)
    local remainder = rowid%row_page_range
    local rowid_k = math.ceil(rowid/row_page_range)
    if remainder == 0 then rowid_k = rowid_k + 1 end
    return rowid_k
end

local rowid_len_str = string.len(row_page_range) - 1 -- rowid的字符串格式长度 1000 => 4-1
-- rowid 偏移量
local rowid_offset = function(rowid, rowid_k)
    rowid = rowid - (rowid_k-1)*row_page_range
    -- 补0
    return strpatch(rowid, "0", rowid_len_str)
end

-- 创建索引
local set_index = function(row, rowid)
    if not indexFields or indexFields == {} then return end
    for indexField in pairs(indexFieldsTable) do
        local indexEngine = indexFieldsTable[indexField]
        while true do
            -- 不存在索引字段
            local indexValue = get_value_by_field(row, indexField)
            if not indexValue then
                break
            end
            if indexEngine == 'bitmap' then
                -- bitmap索引，适合聚合度较强（维度小）的离散数据，如渠道、区服、有限类型
                local bitmap_key = db_key .. "." .. table_key .. ".bitmap." .. indexField .. "." .. indexValue
                redis.call("setbit", bitmap_key, rowid, 1)
            else
                if indexEngine == 'hash' then

                    -- 哈希索引，适合聚合度小（维度多）的离散数据，如广告关键字
                    local index_key = db_key .. "." .. table_key .. ".hash." .. indexField
                    local index_value = indexValue
                    local rowidstr = redis.call("hget", index_key, index_value)
                    if rowidstr then
                        rowidstr = rowidstr .. "," .. rowid
                    else
                        rowidstr = rowid
                    end
                    redis.call("hset", index_key, index_value, rowidstr)

                else
                    -- 普通索引，适合范围查找的连续数据，如时间
                    -- indexValue 支持长度为10的索引值，不够补0
                    local row_value = strpatch(indexValue)

                    local index_key = db_key .. "." .. table_key .. ".index." .. indexField
                    -- 每100条rowid拼成index_value， 减少空间占用，{v_day}:{indexid}:{rowid...}, 如20170214:1:1,2,1,...,100;   20170214:2:101,102,...,200
                    local indexid = redis.call("hget", index_key .. ".indexid", row_value)
                    if not indexid then
                        -- 初始化补0，支持100000条索引
                        indexid = tonumber(strpatch("1", "0", index_id_len_limit, "right"))
                    else
                        indexid = tonumber(indexid)
                    end

                    local last_index_value_key = row_value .. ":" .. indexid

                    -- 查找最后索引的rowid，更新索引值 append and set
                    local index_value_old = redis.call("zrangebylex", index_key, "[" .. last_index_value_key .. ":", "[" .. last_index_value_key .. ";")
                    local index_value_new = ""
                    if index_value_old[1] then
                        -- 删除所有旧set
                        for ivk in pairs(index_value_old) do
                            redis.call("zrem", index_key, index_value_old[ivk])
                        end
                        index_value_new = index_value_old[1] .. "," .. rowid
                        -- 统计value中的rowid个数是否达到100，更新最后index
                        if countstr(index_value_new, ",") >= index_rowid_len_limit then
                            indexid = indexid + 1
                        end
                        redis.call("hset", index_key .. ".indexid", row_value, indexid)
                    else
                        index_value_new = last_index_value_key .. ":" .. rowid
                    end
                    -- 新增index_value_key
                    redis.call("zadd", index_key, 0, index_value_new)
                end
            end
            break
        end
    end
end

-- 格式化压缩记录
-- rows_body[rowid_k] = rows_body[rowid_k] .. row_block_delimiter .. rowid .. ":" .. string.len(row_body) .. row_body
local rows_body = {}
for j,row in pairs(rows) do
    local rowid = tonumber(get_value_by_field(row, "id"))
    if rowid then
        local rowid_k = rowid_key(rowid)
        if not rows_body[rowid_k] then rows_body[rowid_k] = {} end

        local row_body = {}
        for k,v in pairs(row_header) do
            table.insert(row_body, get_value_by_field(row, v))
        end
        local rowid_os = rowid_offset(rowid, rowid_k)
        row_body = cjson.encode(row_body)
        if string.len(row_body) < row_len_limit then
            rows_body[rowid_k][rowid_os] = row_body
            -- 创建索引
            set_index(row, rowid)
        end
    end

end

-- rangeid => ~rowid1:len1[row1]~rowid2:len2[row1]
local row_len_str = string.len(row_len_limit) -- 记录长度的字符串格式长度 1000 => 4
for rowid_k,row_v in pairs(rows_body) do
    -- 判断区间内是否存在该记录
    local rowpage = redis.call("hget", row_key, rowid_k)
    local newrowpage, rowstr_append = "", ""
    if rowpage then
        -- 遍历rowpage全字符串,取出旧记录
        local rowpage_len = string.len(rowpage)
        local block_len, rowid_os, row_len, block_start = 0, 0, 0, 1
        while block_start < rowpage_len do
            rowid_os = string.sub(rowpage, block_start+1, block_start+rowid_len_str)
            row_len = tonumber(string.sub(rowpage, block_start+rowid_len_str+2, block_start+rowid_len_str+1+row_len_str))
            block_len = 1+rowid_len_str+1+row_len_str+row_len
            if not row_v[rowid_os] then
                newrowpage = newrowpage .. string.sub(rowpage, block_start, block_start+block_len-1)
            end
            block_start = block_start+block_len
        end

    end
    -- 遍历新记录
    for rowid_os,row_body in pairs(row_v) do
        rowstr_append = rowstr_append .. row_block_delimiter .. rowid_os .. ":" .. strpatch(string.len(row_body), "0", row_len_str) .. row_body
    end
    -- 添加/ 更新记录
    redis.call("hset", row_key, rowid_k, newrowpage .. rowstr_append)
end

return true

