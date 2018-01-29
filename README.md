# 安装和适用

版本：python 2.7.*

## LINUX环境安装

### 安装pip
yum install -y python-setuptools
sudo easy_install pip

pip源 vi ~/.pip/pip.conf
[global]
index-url=https://pypi.python.org/simple
豆瓣：http://pypi.douban.com/simple
v2ex：http://pypi.v2ex.com/simple


### 安装编译器
yum install -y gcc python-devel libffi-devel gcc-gfortran openssl-devel
### 安装依赖库
yum install -y blas blas-devel lapack lapack-devel atlas atlas-devel mysql-devel  --nogpgcheck
### 安装扩展库
pip install MySQLdb  或者  pip install MySQL-python  （MySQL-python包含MySQLdb）
pip install DBUtils
https://pypi.python.org/pypi/DBUtils/ 下载解压安装 python setup.py install
pip install redis
pip install paramiko

### 科学计算库安装
pip install numpy
pip install scipy
pip install matplotlib

## Docker 安装
git clone https://github.com/mpdesign/mp-docker.git
docker-compose up -d analysis


## usage:
master执行所有slave
配置文件：config.py
拷贝工程代码：scopy [-f 绝对路径 ]  默认拷贝整个工程代码至所有slave节点
命令帮助信息：help
脚本运行状态：top
```

启动命令：{project_path}/[master|slave] [start|stop|restart] [job|layer.job] -d [dateRange] -t [oneTask,twoTask...] -g app_id
1）-d dateRange 时间范围
    按天 20150420,20150421
    按月 201504,201505
    按周 2015010,2015020(尾部加0以作区别于月)
2）-t taskName 指定重跑的子任务名
3）-g 指定游戏ID
4) -now 立刻执行
```




