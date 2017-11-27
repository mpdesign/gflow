drop PROCEDURE load_test_data;
delimiter //

CREATE PROCEDURE load_test_data()
  begin
    declare i int default 1;
    declare j int default 1;

    while i < 10000
    do
        set j = 1;
		    set @exesql = 'insert into d_test values ';
		    set @exedata = "";
        while j < 366
		    do
--           set @exedata = concat(@exedata, ",(" , "''", ",", DAYOFYEAR(FROM_UNIXTIME(UNIX_TIMESTAMP()-j*24*3600, '%Y-%m-%d')), ",", UNIX_TIMESTAMP(), ",", FLOOR((RAND() * 366)) + 1, ")");
          set @exedata = concat(@exedata, ",(" , "''", ",", FROM_UNIXTIME(UNIX_TIMESTAMP()-j*24*3600, '%Y%m%d'), ",", UNIX_TIMESTAMP(), ",", FLOOR((RAND() * 366)) + 1, ")");
          set j = j + 1;
        end while;
        set @exedata = SUBSTRING(@exedata, 2);
        set @exesql = concat(@exesql, @exedata);
        prepare stmt from @exesql;
        execute stmt;
        DEALLOCATE prepare stmt;
        set i = i + 1;
    end while;
  end
  //
delimiter ;
call load_test_data();

/** InnoDB报表 按系统平台os分区 按年或数据量分表 **/
drop table r_test_hash;
CREATE TABLE `r_test_hash` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `os` TINYINT(3) NOT NULL COMMENT '1ios 2and 3win ...',
  `v_day` int(10) unsigned NOT NULL,
  `sid` int(10) unsigned NOT NULL,
  `channel_id` varchar(10) NOT NULL,
  PRIMARY KEY (`id`, `os`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='报表'
PARTITION BY HASH (os) PARTITIONS 5 ;

/** ARCHIVE 表，按天分区，按年分表 **/
/** hash分区 **/
drop table d_test_hash;
CREATE TABLE `d_test_hash` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `d` smallint(10) unsigned NOT NULL COMMENT '一年中的第几天, 1-366',
  `v_time` int(10) unsigned DEFAULT '0' COMMENT '时间戳',
  `meta` text COMMENT '扩展元数据',
  KEY (`id`)
) ENGINE=ARCHIVE AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='元数据表hash分区'
PARTITION BY HASH (d) PARTITIONS 366 ;
/**自增字段必须为索引，索引必须是自增字段，不能创建主键，并且最多只有一个索引**/
/** list分区 **/
drop table d_test_list;
CREATE TABLE `d_test_list` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `v_day` int(10) unsigned NOT NULL COMMENT '天,20170101',
  `v_time` int(10) unsigned DEFAULT '0' COMMENT '时间戳',
  `meta` text COMMENT '扩展元数据',
  KEY (`id`)
) ENGINE=ARCHIVE AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='元数据表list分区'
PARTITION BY LIST(v_day) (
PARTITION p20170101 VALUES IN (20170101)
);
/** 判断是否存在分区 **/
select partition_name from information_schema.partitions  where table_schema=schema() and table_name='d_test' and partition_name='p0';
/** 增加分区 **/
ALTER TABLE d_test_list ADD PARTITION (PARTITION p1 VALUES IN (1));

/** 查看分区表数据 **/
select partition_name , partition_expression , partition_description , table_rows, DATA_LENGTH from information_schema.partitions  where table_schema = schema() and table_name='d_test';

explain partitions select * from t_test partition (p93);

ALTER TABLE table_name DROP PARTITION p_name;

/**查看表大小**/
select TABLE_NAME,DATA_LENGTH from information_schema.TABLES WHERE TABLE_SCHEMA =schema() \G;