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
mysql 连接池 pip install DBUtils
https://pypi.python.org/pypi/DBUtils/ 下载解压安装 python setup.py install
pip install redis
pip install paramiko

### 科学计算库安装
pip install numpy
pip install scipy
pip install matplotlib  42M

## Docker 安装
git clone https://github.com/mpdesign/docker-mp.git
docker-compose up -d


## 使用

usage:
master执行所有slave
```
{project_path}/master [start|stop|restart] [job]
```
拷贝工程代码：scopy [-f 绝对路径 ]  默认拷贝整个工程代码至所有slave节点
命令帮助信息：help
脚本运行状态：top

执行当前slave
```
{project_path}/slave [start|stop|restart] [job]
```

任务编写规范实例：demo
新建任务调度文件：{project_path}/work/demo/demo.py
新建子任务文件：  {project_path}/work/demo/one.py

在demo.py文件注册子任务名：oneTask
```
from work.__task__ import *
class demoJob(taskInterface):
    def __init__(self):
        taskInterface.__init__(self)
        # 任务名注册表，指定要执行的脚本
        self.registerTask = [
            'oneTask'
            ]
```

在task1.py编写具体的业务逻辑，必须实现以下三个方法
```
from work.__task__ import *
class oneTask(demoJob):

    def beforeExecute(self):
        # 定时运行
        # 格式：mwdHM
        # 每周一2点执行
        # self.atExecute = 'w1d2'

        # 间隔运行
        # 格式：秒
        # self.sleepExecute = 3600

        # 只执行一次
        self.breakExecute = True


    # 自定义任务列表
    def mapTask(self):
        # 返回所有任务，用于指派给所有slave
        # 格式：list
        return self.assignTask()

        # 指定只允许某个slave运行该任务
        # return ip


    # 默认执行方法
    def execute(self, myTask=[]):
        # 获取当前slave任务列表
        # myTask

        # 获取当前时间戳，适用于重跑数据
        # self.curTime()

        # 获取当前slave文件任务列表
        # self._myFile

        print 'complete'
```
```
配置文件：config.py
可执行权限：chmod 777 {project_path}/[master|slave]
启动命令：{project_path}/[master|slave] start demo
停止命令：{project_path}/[master|slave] stop demo
启动命令：{project_path}/[master|slave] restart demo
重跑命令：{project_path}/[master|slave] restart demo -d [dateRange]
批量启动：{project_path}/[master|slave] restart demo -t [oneTask,twoTask...]
1）-d dateRange 时间范围
    按天 20150420,20150421
    按月 201504,201505
    按周 2015010,2015020(尾部加0以作区别于月)
2）-t taskName 指定重跑的子任务名
3）-g 指定游戏ID
4) -now 立刻执行
```

[new version]
建立作业与任务关系
进程级别单例
进程pid文件增加多进程多用户支持
集群处理返回结果集
兼容连接池模式: redis/mysql
缓存key采用哈希表 hash table
在线  活跃  留存超过百万级别的采用bitmap，每日一个键，否则采用集合

== 消息流 ==
消息的发布与订阅 Pub/Sub
管道传输（pipeline）批次执行命令

== 数据流 ==
配置类数据应读写均在redis，守护一个mysql落地脚本

== 事务流 ==
redis响应前端，GF后端排队处理，建立消息补偿机制

== 预分析流 ==
按需提交任务或预先分析


业务优化：
1、redis/mysql config 、 save 分三层存储：元数据库、标签库、报表库，预留日志库层
3、当其中一款游戏发生无法连接数据库等异常导致的程序终止，不影响其他游戏任务脚本运行
4、报表数据一致性检验机制
5、重跑数据备份及对比




