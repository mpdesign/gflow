# -*- coding: utf-8 -*-
import re
from common.common import *


# 词汇聚合统计
# 词频统计
def count(lst, separator=[' ', '-', ','], special={}, sort=True, percent=0):
    """
    :param lst: 词汇 list
    :param separator: 分隔符 list
    :param special: 例外 dict
    :param sort: 排序 sort order by count desc
    :param percent: 返回比例 filter percent 0<=percent<=1
    :return: word count dict
    """
    if isinstance(lst, type('')):
        lst = [lst]
    if not lst or not isinstance(lst, type([])):
        return None
    wc = {}

    def _for_spl(j, _sl):
        if j >= len(separator):
            return _sl
        for _s in _sl:
            if separator[j] == '-' and _s.find('-') < 3:
                #不分割,进行下一个字符类型分割
                _ssl = [_s]
            else:
                _ssl = _s.split(separator[j])
            _ssl = _for_spl(j+1, _ssl)
            if _ssl:
                for _ss in _ssl:
                    #清除空格和转义字符
                    _ss = _ss.strip('\\ "')
                    _ss = _ss.replace("(", "")
                    _ss = _ss.replace(")", "")
                    #小于3的扔掉
                    if len(_ss) >= 3:
                        sl.append(_ss)

    for s in lst:
        #循环分割s为列表sl
        sl = []
        s = special[s] if s in special.keys() else s
        _for_spl(0, [s])
        #sl合并到wl
        for stri in sl:
            stri = stri.lower()
            if stri not in wc.keys():
                wc[stri] = 1
            else:
                wc[stri] += 1
    #排序后返回列表
    if sort:
        wc = sorted(wc.items(), lambda x, y: cmp(y[1], x[1]))
        #wc = map(lambda x: x[0], wc)
        wl = len(wc)
        if wl < 1:
            return []
        avg = 0
        if 0 < percent < 1:
            wl_percent = wl*percent
            i = 0
            for x in wc:
                if i >= wl_percent:
                    break
                i += 1
                avg = x[1]
        elif percent >= 1:
            avg = 0
        else:
            #默认取平均值
            ws = sum([x[1] for x in wc])
            avg = ws/wl
        #提取词频大于平均值的关键字
        wc = [x for x in wc if x[1] >= avg]
    return wc

#根据关联度（关联次数），聚合词汇，被聚合的词汇将在字典中抛弃分类到列表中
#def group(lst):


#按词频相似归类 1、设定词长like_len；2、分割空格和-，统计词频；3、优先匹配高词频，小于词长的满足最左原则，再聚合大于词长的关键字;
def groupByLike(lst, separator=[' ', '-', ','], special={}, like_len=5):
    groups = {}
    clst = special if special else lst
    wc = count(clst, separator=separator)
    for item in lst:
        item = item.lower()
        item2 = special[item] if item in special.keys() else item
        item2 = item2.replace('(', '').replace(')', '')
        for w in wc:
            w = w[0]
            try:
                if len(w) < like_len or intval(w) > 0:
                    #小于like_len或纯数字满足最左匹配
                    m = re.match(w, item2)
                else:
                    m = re.search(w, item2)
            except Exception:
                continue
            if m:
                if w not in groups.keys():
                    groups[w] = []
                groups[w].append(item)
                break
    return groups
