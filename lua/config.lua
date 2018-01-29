-- 执行 r.excute_command('SHUTDOWN NOSAVE') 强制终止redis服务，执行 r.excute_command('SCRIPT KILL')  可终止只读操作的脚本
-- 公共配置文件
local config = {}

-- 表配置
config["table.name"] = {}

-- 记录集hset
-- db.table.row => pageid => ~rowidoffset1:len1[row1]~rowidoffset2:len2[row1]
-- 每1000个区块组成一个页记录
config["row_page_range"] = 1000
-- config["table.name"]["row_hash_range"] = 1000
-- 区块 = {分隔符}{rowid偏移量}:{row长度}{row}
-- row_block分隔符
config["row_block_delimiter"] = "~"
-- 记录最大长度，字节
config["row_len_limit"] = 1000

-- 普通索引 zset 索引长度 10位字段值+6位索引ID+100个rowid
-- db.table.index.fieldName =>  0, fieldValue:indexid:rowid1,...rowidn
config["index_zset_field_len_limit"] = {10, 6, 100 }
-- config["table.name"]["index_zset_field_len_limit"] = {10, 6, 100 }

-- bitmap索引
-- db.table.bitmap.fieldName.fieldValue => 0000001

-- hash 索引
-- db.table.hash.fieldName => fieldValue, rowid1,...rowidn

-- 特殊字段指定索引引擎, 支持三种引擎： bitmap  hash  index
config["index"] = {}
config["index"]["channel_id"] = "bitmap"
config["index"]["sid"] = "bitmap"
config["index"]["adid"] = "hash"
config["index"]["pid"] = "hash"
config["index"]["v_day"] = "index"
config["index"]["v_week"] = "index"
config["index"]["v_month"] = "index"

-- 配置key
local cache_key = "lua.config"
redis.call('set', cache_key, cjson.encode(config))
-- redis.call('expire', cache_key, 300)
return 1