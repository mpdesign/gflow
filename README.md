# 安装和适用

版本：python 2.7.*


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




