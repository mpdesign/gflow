# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import numpy as np

#导入数据
data = np.loadtxt('C:\\Python27\\fakedata.txt')
#X轴显示间隔
step = 3


def set_plot(sax, sdata):
    ds = sdata.shape
    for i in range(1,ds[1]):
        y = sdata[:,i]
        sax.plot(rx,y)


def set_x(sax, xx):
    #数据个数
    l = xx.shape
    xticklabels = []
    xticks = []

    for n in range(1,l[0],step):
        d = str(int(xx[n]))
        dk = "%s-%s-%s" % (d[0:4], d[4:6], d[6:8] )
        xticklabels.append(dk)
        xticks.append(n)
    #重置x轴比例
    rx = range(0,l[0])
    sax.set_xticks(xticks)
    sax.set_xticklabels(xticklabels,rotation=15)
    return rx
 

# plot the first column as x, and second column as y
x = data[:,0]


# trick to get the axes
ax = plt.subplot()

#坐标标识
plt.xlabel(u"日期")  
plt.ylabel(u'指标')

#X显示日期
rx = set_x(ax, x)


# plot data
#根据传入数据的列数设定曲线个数
set_plot(ax, data)
   

# show the figure
plt.show()




