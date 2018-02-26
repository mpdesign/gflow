# -*- coding: utf-8 -*-
# Filename: config.py

# -----------------------------------
# Revision:     2.0
# Date:         2018-02-24
# Author:       mpdesign
# description:  配置脚本
# -----------------------------------

# ip127.0.0.1 is not allowed to be set to the master or slave node
# 默认运算节点
DEFAULT_NODE = '172.18.0.101'

# master node
MASTER_NODE = "172.18.0.101"

# slave node
SLAVE_NODE = [
    {'ip': '172.18.0.101', 'port': '22', 'user': 'root', 'password': ''},
    {'ip': '172.18.0.102', 'port': '22', 'user': 'root', 'password': ''}
]