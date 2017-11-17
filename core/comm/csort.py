# -*- coding: utf-8 -*-
# Filename: csort.py

# -----------------------------------
# Revision:     2.0
# Date:         2017-11-06
# Author:       mpdesign
# description:  排序算法库
# -----------------------------------

import sys


# 快速排序算法
def quickSort(data, order='asc', by=''):
    if len(data) > 1000:
        sys.setrecursionlimit(3000)
    if len(data) > 3000:
        return
    return subQuickSort(data, 0, len(data)-1, order=order, by=by)


def subQuickSort(data, start_index, end_index, order='asc', by=''):
    i = start_index
    j = end_index
    isdict = True if isinstance(data[j], type({})) and by in data[j].keys() else False
    if i >= j:
        return data
    flag = data[i][by] if isdict else data[i]
    flagitem = data[i]
    while i < j:
        while i < j:
            compare_value = data[j][by] if isdict else data[j]
            rightfind = (compare_value < flag) if order == 'asc' else (compare_value > flag)
            if rightfind:
                break
            j -= 1
        data[i] = data[j]

        while i < j:
            compare_value = data[i][by] if isdict else data[i]
            leftfind = (compare_value > flag) if order == 'asc' else (compare_value < flag)
            if leftfind:
                break
            i += 1
        data[j] = data[i]

    data[i] = flagitem
    subQuickSort(data, start_index, i - 1, order=order, by=by)
    subQuickSort(data, j + 1, end_index, order=order, by=by)
    return data