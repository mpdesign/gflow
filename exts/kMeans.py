 # -*- coding: utf-8 -*-
# Filename: kMeans.py
# -----------------------------------
# Revision:         2.0
# Date:             2015-04-01
# Author:           84086365
# description: K均值聚类算法
# ------------------------------------------------------------------------
# 无监督自主划分未分类的数据

# 1、任意选择k个点，作为初始的聚类中心。
# 2、遍历每个对象，分别对每个对象求他们与k个中心点的距离，把对象划分到与他们最近的中心所代表的类别中去，这一步我们称之为“划分”。
# 3、对于每一个中心点，遍历他们所包含的对象，计算这些对象所有维度的和的中值，获得新的中心点。
# 4、计算当前状态下的损失(用来计算损失的函数叫做Cost Function，即价值函数)，如果当前损失比上一次迭代的损失相差大于某一值（如1），
# 则继续执行第2、3步，知道连续两次的损失差为某一设定值为止（即达到最优，通常设为1）。
#
# 价值函数（Cost Function）
# 每一次选取好新的中心点，我们就要计算一下当前选好的中心点损失为多少，这个损失代表着偏移量，越大说明当前聚类的效果越差，计算公式称为：
# L(C) = sum(for j in K: sum_(for i in j: power(x{i} - c{j}, 2))
# 其中，x{i} 表示某一对象，c_{j} 表示该对象所属类别的中心点。整个式子的含义就是对各个类别下的对象，求对象与中心点的差平方，把所有的差平方求和就是  L(C) 。

# 评价标准
# 采用聚类的数据，通常没有已知的数据分类标签，所以通常就不能用监督学习中的正确率、精确度、召回度来计算了（如果有分类标签的话，也是可以计算的）。
# 常用于聚类效果评价的指标为：Davies Bouldin Index，它的表达式可以写为：
#  DB = ( for i,j in n:if i != j: sum( (rho{i}-rho{j})/d(c{i}-c{j}) ) ) / n
# 其中rho_{i}和rho_{j}  都表示i,j两个分类中的所有对象到中心点的平均距离，而分母中的 c{i} 和  c{j} 分别表示i,j两个分类的中心点之间的距离。
# 整个表达式的含义就是，聚类效果越好，两个分类间的距离应该越远，分类内部越密集。
# -------------------------------------------------------------------------

from numpy import *
import time
#import matplotlib.pyplot as plt


# 两点间的欧式距离
def euclDistance(vector1, vector2):
    return sqrt(sum(power(vector2 - vector1, 2)))


# 随机初始K个中心点
def initCentroids(dataSet, k):
    numSamples, dim = shape(dataSet)
    centroids = mat(zeros((k, dim)))
    for i in range(k):
        index = int(random.uniform(0, numSamples))
        centroids[i, :] = array(dataSet)[index, :]
    return centroids


# k-means cluster
def kMeans(dataSet, k):
    # 样本数量
    numSamples = shape(dataSet)[0]
    # 第一列存储数据样本所属的簇
    # 第二列存储该数据样本和它的中心（质心）点之间的误差
    # 初始一个numSamples行，2列的二维矩阵 mat=二维array
    clusterAssment = mat(zeros((numSamples, 2)))
    clusterChanged = True

    # step 1: 初始中心点
    centroids = initCentroids(dataSet, k)

    while clusterChanged:
        clusterChanged = False
        # 遍历所有数据样本
        for i in xrange(numSamples):
            minDist = -1
            minIndex = 0
            # step 2: 对每个数据样本计算所有中心点的距离，求最小值，找到距离最近的中心点minIndex
            for j in range(k):
                distance = euclDistance(centroids[j, :], array(dataSet)[i, :])
                if minDist < 0:
                    minDist = distance
                    continue
                if distance < minDist:
                    minDist = distance
                    minIndex = j

            # step 3: 更新簇，数据样本聚类到对应的簇
            if clusterAssment[i, 0] != minIndex:
                clusterChanged = True
                clusterAssment[i, :] = minIndex, minDist**2
        # step 4: 更新中心点，计算簇集合里的数据点求平均值，当作新的中心点
        for j in range(k):
            # nonzero: 获取等j的数据点
            pointsInCluster = array(dataSet)[nonzero(array(clusterAssment)[:, 0] == j)[0]]
            # 求均值
            centroids[j, :] = mean(pointsInCluster, axis=0)
    resultSet = []
    for j in range(k):
        # nonzero: 获取等j的数据点
        pointsInCluster = array(dataSet)[nonzero(array(clusterAssment)[:, 0] == j)[0]]
        resultSet.append(pointsInCluster.tolist())
    return centroids, clusterAssment, resultSet

# show your cluster only available with 2-D data
# def showCluster(dataSet, k, centroids, clusterAssment):
#     numSamples, dim = dataSet.shape
#     if dim != 2:
#         print "Sorry! I can not draw because the dimension of your data is not 2!"
#         return 1
#
#     mark = ['or', 'ob', 'og', 'ok', '^r', '+r', 'sr', 'dr', '<r', 'pr']
#     if k > len(mark):
#         print "Sorry! Your k is too large! please contact Zouxy"
#         return 1
#
#     # draw all samples
#     for i in xrange(numSamples):
#         markIndex = int(clusterAssment[i, 0])
#         plt.plot(dataSet[i, 0], dataSet[i, 1], mark[markIndex])
#
#     mark = ['Dr', 'Db', 'Dg', 'Dk', '^b', '+b', 'sb', 'db', '<b', 'pb']
#     # draw the centroids
#     for i in range(k):
#         plt.plot(centroids[i, 0], centroids[i, 1], mark[i], markersize = 12)
#
#     plt.show()