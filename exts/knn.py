# -*- coding: utf-8 -*-
# Filename: knn.py
# -----------------------------------
# Revision:         2.0
# Date:             2015-04-02
# Author:           84086365
# description: K临近算法，数据集为浮点型
# ------------------------------------------------------------------------
# 根据已知分类数据监督训练，预测指定数据的分类

# 1）计算已知类别数据集中的点与当前点之间的距离；
#
# 2）按照距离递增次序排序；
#
# 3）选取与当前点距离最小的k个点；
#
# 4）确定前k个点所在类别的出现频率；
#
# 5）返回前k个点出现频率最高的类别作为当前点的预测分类。
# -------------------------------------------------------------------------

from numpy import *


def kNNClassify(newInput, dataSet, labels, k):
    """
    newInput: 测试集 array
    dataSet： 已知分类集 array
    labels：  已知分类集标签 list
    k：       距离最小的K个点
    """
    numSamples = dataSet.shape[0]

    ## step 1: 欧式距离
    # 对测试数据重复构建一个数组，重复numSamples行，列不变，对应已知分类集的行数，求差的平方
    diffData = tile(newInput, (numSamples, 1)) - dataSet
    squaredDiff = diffData ** 2
    # axis=1 表示各行分别求和
    squaredDist = sum(squaredDiff, axis=1)
    distance = squaredDist ** 0.5

    ## step 2: 距离排序, 数组值从小到大的索引值
    sortedDistIndices = argsort(distance)

    classCount = {} # define a dictionary (can be append element)
    for i in xrange(k):
        ## step 3: 选出距离最近的K个数据
        voteLabel = labels[sortedDistIndices[i]]

        ## step 4: 计算前k个点所在类别的出现频率
        classCount[voteLabel] = classCount.get(voteLabel, 0) + 1

    ## step 5: 取出现频率最大的标签
    maxCount = 0
    for key, value in classCount.items():
        if value > maxCount:
            maxCount = value
            maxIndex = key

    return maxIndex


# 创建数据集
# def createDataSet():
#     # create a matrix: each row as a sample
#     group = array([[1.0, 0.9], [1.0, 1.0], [0.1, 0.2], [0.0, 0.1]])
#     labels = ['A', 'A', 'B', 'B'] # four samples and two classes
#     return group, labels
# dataSet, labels = createDataSet()
#
# testX = array([1.2, 1.0])
# k = 3
# outputLabel = kNNClassify(testX, dataSet, labels, 3)
# print "Your input is:", testX, "and classified to class: ", outputLabel
#
# testX = array([0.1, 0.3])
# outputLabel = kNNClassify(testX, dataSet, labels, 3)
# print "Your input is:", testX, "and classified to class: ", outputLabel