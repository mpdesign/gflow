# -*- coding: utf-8 -*-
# Filename: naiveBayes.py
# -----------------------------------
# Revision:         2.0
# Date:             2015-03-20
# Author:           84086365
# description: 朴素贝叶斯算法，分类属性较少，数据集为字符串离散型
# -----------------------------------
# 算法一般流程
# 1.数据的收集
# 2.数据的准备：数值型或布尔型
# 3.分析数据
# 4.训练算法：计算不同的独立特征的条件概率
# 5.测试算法：计算错误率
# 6.使用算法：以实际应用为驱动

# 朴素贝叶斯算法：
# 1.计算各个独立特征在各个分类中的条件概率
# 2.计算各类别出现的概率
# 3.对于特定的特征输入，计算其相应属于特定分类的条件概率
# 4.选择条件概率最大的类别作为该输入类别进行返回

# 贝叶斯条件概率公式：
# p(c{i}|x,y) = p(x,y|c{i})p(c{i})/p(x,y) = p(x,y|c{i})p(c{i})/p(x)p(y)

# -----------------------------------

from numpy import *


def loadDataSet():
    postingList = [
        ['my', 'dog', 'has', 'flea', 'problems', 'help', 'please'],
        ['maybe', 'not', 'take', 'him', 'to', 'dog', 'park', 'stupid'],
        ['my', 'dalmation', 'is', 'so', 'cute', 'I', 'love', 'him'],
        ['stop', 'posting', 'stupid', 'worthless', 'garbage'],
        ['mr', 'licks', 'ate', 'my', 'steak', 'how', 'to', 'stop', 'him'],
        ['quit', 'buying', 'worthless', 'dog', 'food', 'stupid']
    ]
    classVec = [0, 1, 0, 1, 0, 1]
    return postingList, classVec


# 训练集特征值除重，返回一维列表
def createVocabList(dataSet):
    vocaSet = set([])
    for doc in dataSet:
        vocaSet = vocaSet | set(doc)
    return list(vocaSet)


# 计算测试集每行特征值对应训练集所有特征值列表vocabList是否存在，返回等同训练集行数、vocabList列数的二维列表
def setOfWords2Vec(vocabList, inputSet):
    returnVec = []
    for doc in inputSet:
        tmpVec = [0]*len(vocabList)
        for word in doc:
            if word in vocabList:
                tmpVec[vocabList.index(word)] = 1
            else:
                print "the word: %s is not in my vacobulary!" % word
        returnVec.append(tmpVec)
    return returnVec


def bagOfWords2VecMN(vocabList, inputSet):
    returnVec = []
    for doc in inputSet:
        tmpVec = [0]*len(vocabList)
        for word in doc:
            if word in vocabList:
                if word in vocabList:
                    tmpVec[vocabList.index(word)] += 1
                else:
                    print "the word: %s is not in my vacobulary"%word
        returnVec.append(tmpVec)
    return returnVec


def trainNB0(trainMatrix, trainCategory):
    numTrainDocs = len(trainMatrix)
    numWords = len(trainMatrix[0])
    pAbusive = sum(trainCategory)/float(numTrainDocs)
    '''
    p0Num = zeros(numWords)
    p1Num = zeros(numWords)
    '''
    p0Num = ones(numWords)
    p1Num = ones(numWords)
    p0Denom = 2.0
    p1Denom = 2.0
    for i in range(numTrainDocs):
        if trainCategory[i] == 1:
            p1Num += trainMatrix[i]
            p1Denom += sum(trainMatrix[i])
        else:
            p0Num += trainMatrix[i]
            p0Denom += sum(trainMatrix[i])
    p1Vect = p1Num/p1Denom
    p0Vect = p0Num/p0Denom
    return p0Vect, p1Vect, pAbusive


def classifyNB(vec2Classify, p0Vec, p1Vec, pClass):
    p1 = sum(vec2Classify*p1Vec)+log(pClass)
    p0 = sum(vec2Classify*p0Vec)+log(1.0-pClass)
    if p1 > p0:
        return 1
    else:
        return 0


def testingNB():
    listOPosts, listClasses = loadDataSet()
    myVocabList = createVocabList(listOPosts)
    trainMat = setOfWords2Vec(myVocabList, listOPosts)
    p0V, p1V, pAb = trainNB0(array(trainMat), array(listClasses))
    result = []
    testSet = [['love', 'my', 'dalmation'], ['stupid', 'garbage']]
    testVec = setOfWords2Vec(myVocabList, testSet)
    for test in testVec:
        tmp_r = classifyNB(test, p0V, p1V, pAb)
        result.append(tmp_r)
    for i in range(len(testSet)):
        print testSet[i]
        print "The class of it is: "+str(result[i])



