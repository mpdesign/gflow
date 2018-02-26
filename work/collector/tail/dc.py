# -*- coding: utf-8 -*-
import urlparse
from tail import *


class dcTask(tailJob):

    def beforeExecute(self):
        self.breakExecute = True

    def taskDataList(self):
        return [{'assign_node': curNode()}]

    def execute(self, myTaskDataList=[]):
        singleton.getinstance('tail').conf(
            sourceFile='/data/logs/nginx/api_dc.access_log',
            atSink=['H05', 'd15'],
            channelPoolLen=10,
            uploadPoolLen=3,
            chunk_size=1000000,
            compress=False,
            sinkFolder='/gflow/logs'
        ).register_callback(self.doLine, self.dcSchema()).follow()

    @staticmethod
    def doLine(line='', schema=None):
        """
        #:parameter line
        #:return topic_name, data_time, log_time, row
        # 回调处理行数据
        """
        line_split = line.split(' ')
        # IP
        _ip = line_split[0]

        # 日志时间
        if not line_split[3][1:]:
            return False
        timeArray = time.strptime(line_split[3][1:], "%d/%b/%Y:%H:%M:%S")
        log_time = time.mktime(timeArray)

        # url
        _url = line_split[6]
        _url_list = _url.split('?')
        _uri = _url_list[0]

        # 主题名
        topic_name = _uri.split('/')[-1]
        if topic_name not in schema:
            return False

        # query data
        # 去掉引号
        query_data = line_split[-3][1:-1] if len(_url_list) == 1 else _url_list[1]
        query_dict = urlparse.parse_qs(query_data)
        query_row = ''
        for column in schema[topic_name]:
            query_row += query_dict[column][0] + '\t' if column in query_dict else '' + '\t'

        # 数据时间
        data_time = log_time if 'time' not in query_dict else floatval(query_dict['time'][0])

        # app_id
        app_id = 'default' if 'app_id' not in query_dict else query_dict['app_id'][0]

        # 头部
        _header = line_split[-4][1:-1]

        # 组成行字符串
        row = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(log_time)) + '\t' \
              + _ip + '\t' + _header + '\t' + _uri + '\t' + query_row + '\n'
        return app_id, topic_name, data_time, log_time, row

    # 按照以下格式顺序存储
    def dcSchema(self):
        """
        create hive external table
        create external table d_record_10010(log_date string, ip string, header string, uri string, v_time int, channel_id string,pid string, level int, sid int, vip int, missionID int, itemID int, itemNum int, currencyID int, currencyNum int, currencyRemain int, itemRemain int)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  STORED AS TEXTFILE  location '/gflow/logs/10010/record/';
        """
        schemes = dict()

        schemes['login'] = ["time", "channel_id", "pid", "did", "uid", "sid", "ip"]

        schemes['show'] = ['time', 'wid', 'adid', 'ip', 'channel_id', 'user_agent', 'unique_code']

        schemes['landing'] = ['time', 'wid', 'adid', 'ip', 'channel_id', 'user_agent', 'unique_code']

        schemes['click'] = ['time', 'mac', 'idfa', 'ip', 'channel_id', 'sid', 'adext', 'adid']

        schemes['activity'] = ['time', 'did', 'screen', 'osv', 'hd', 'gv', 'mac', 'idfa', 'ip', 'channel_id', 'sid', 'isbreak', 'ispirated', 'adid', 'wid', 'ext']

        schemes['activityCallback'] = ['time', 'channel_id', 'callback_url', 'callback_result']

        schemes['user'] = ["time", "channel_id", "did", "uid", "username", "ip", "type", "gender", "adid"]

        schemes['player'] = ["time", "channel_id", "did", "pid", "uid", "sid", "newdid", "pname", "level", "last_login_day", 'adid']

        schemes['record'] = ["time", "channel_id", "pid", "level", "sid", "vip", "missionID", "itemID", "itemNum", "currencyID", "currencyNum", "currencyRemain", "itemRemain"]

        schemes['mission'] = ["time", "channel_id", "pid", "level", "sid", "missionID", "status", "level_1", "level_2", "level_3"]

        schemes['event'] = ["time", "channel_id", "pid", "did", "sid", "eventID", "value"]

        schemes['playerLevel'] = ["time", "pid", "level", "sid", "channel_id"]

        schemes['iap'] = ['time', 'did', 'oid', 'sid', 'channel_id', 'pid', 'info', 'key', 'value', 'ip']

        return schemes