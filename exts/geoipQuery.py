# -*- coding: utf-8 -*-
# Filename: geoipQuery.py
# ----------------------------------
# Revision:     1.0
# Date:         2017-11-30
# Author:       chensj
# Description:  IP智能数据库
# -------------------------------------------------
# 共有8种类型数据库（country,city,anonymous_ip,asn,connect_type,domain,enterprise,isp),当前版本仅针对city

import geoip2.database
from core.comm.common import *

class geoipQuery(object):

    # 初始化,载入离线数据库文件地址及类型
    def __init__(self, path=MAXMIND_DB_CONFIG['path_city'], type='city'):
        # self.path = path
        self.reader = None
        self.response = None
        self.connect_db(path)
        if hasattr(self.reader, type):
            # self.response = getattr(self.reader, type)(ip)
            self.type = type
        else:
            output('mmdb 数据库类型: %s 与 数据库文件: %s不匹配' % (type, path))

    # 连接mmdb数据库
    def connect_db(self, path):
        if not self.reader:
            try:
                self.reader = geoip2.database.Reader(path)
            except  Exception,e:
                output('mmdb exception '+str(e))
        return self

    # 根据ip获取该ip对应的国家
    def query_country_by_ip(self, ip):
        country_name = ''
        try:
            response = getattr(self.reader, self.type)(ip)
            country_name = response.country.names['zh-CN']
        except Exception,e:
            output('mmdb exception '+str(e))
        return country_name

    # 根据ip获取该ip对应的省份
    def query_province_by_ip(self, ip):
        province_name = ''
        try:
            response = getattr(self.reader, self.type)(ip)
            province_name = response.subdivisions.most_specific.names['zh-CN']
        except Exception,e:
            output('mmdb exception '+str(e))
        return province_name

    # 根据ip获取该ip对应的城市
    def query_city_by_ip(self, ip):
        city_name = ''
        try:
            response = getattr(self.reader, self.type)(ip)
            city_name = response.city.names['zh-CN']
        except  Exception,e:
            output('mmdb exception '+str(e))
        return city_name

    # 根据ip获取经度坐标
    def query_latitude_by_ip(self, ip):
        latitude = ''
        try:
            response = getattr(self.reader, self.type)(ip)
            latitude = response.location.latitude
        except  Exception,e:
            output('mmdb exception '+str(e))
        return latitude

    #根据ip获取纬度坐标
    def query_longitude(self, ip):
        longitude = ''
        try:
            response = getattr(self.reader, self.type)(ip)
            longitude = response.location.longitude
        except  Exception,e:
            output('mmdb exception '+str(e))
        return longitude


    #关闭mmdb数据库
    def close_db(self):
        if self.reader:
            try:
                self.reader.close()
            except  Exception,e:
                output('mmdb exception'+str(e))






