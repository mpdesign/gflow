# -*- coding: utf-8 -*-
# Filename: pcode.py

# -----------------------------------
# Revision:         1.0
# Date:             2015-04-23
# Author:           mpdesign
# description:      可逆编码算法
# -----------------------------------

import math

class pcode(object):

    # key长度不能超过6位
    def __init__(self, key="youli"):
        self.__src_key = ''
        self.__key = ''
        # 不能超过19位
        self.__xorlen = 0
        self.reset_key(key)

    # 密码字符串建议不超过15位, 每__xorlen位异或运算,运算后.连接
    def encode(self, value):
        ascii_value = str(self.__get_strascii(value, True))

        _xorc = int(math.ceil(len(ascii_value)/float(self.__xorlen)))
        _xor = ''
        for i in range(0, _xorc):
            ascii_v = ascii_value[i*self.__xorlen: (i+1)*self.__xorlen]
            # 最后一次异或运算,取加密key第一位,减少长度
            if i == _xorc - 1:
                _xor = '%s.%s' % (_xor, str(int(ascii_v) ^ ord(self.__src_key[0: 1])))
            else:
                _xor = '%s.%s' % (_xor, str(int(ascii_v) ^ self.__key))
        _xor = _xor[1:]
        return _xor
        # return "%d" % (self.__get_strascii(value, True) ^ self.__key)

    def decode(self, pwd):
        """
        解密函数
        """
        pwd_arr = str(pwd).split(".")
        _xor = ''
        pwdc = len(pwd_arr)
        for i in range(0, pwdc):
            ascii_v = pwd_arr[i]
            # 最后一次异或运算,取加密key第一位,减少长度
            if i == pwdc - 1:
                _x = str(int(ascii_v) ^ ord(self.__src_key[0: 1]))
            else:
                _x = str(int(ascii_v) ^ self.__key)

            # 不够3倍数的长度，左边补0
            pad0 = len(_x) % 3
            pad0 = '0' * (3-pad0) if 0 < pad0 < 3 else ""
            _xor = "%s%s%s" % (_xor, pad0, _x)

        return self.__get_strascii(_xor, False)


    def reset_key(self, key):
        """
        重新设置key
        """
        self.__src_key = key
        __key = self.__get_strascii(self.__src_key, True)
        self.__key = int(__key)
        self.__xorlen = max(self.__xorlen, len(str(__key)))
        return self

    def __get_strascii(self, value, bFlag):
        if bFlag:
            return self.__get_str2ascii(value)
        else:
            return self.__get_ascii2str(value)

    def __get_str2ascii(self, value):
        ls = []
        for i in value:
            ls.append(self.__get_char2ascii(i))
        return long("".join(ls))

    @staticmethod
    def __get_char2ascii(char):
        try:
            return "%03.d" % ord(char)
        except (TypeError, ValueError):
            print "key error."
            exit(1)

    def __get_ascii2char(self, ascii):
        if self.is_ascii_range(ascii):
            return chr(ascii)
        else:
            print "ascii error(%d)" % ascii
            exit(1)

    def __get_ascii2str(self, n_chars):
        ls = []
        s = "%s" % n_chars
        n, p = divmod(len(s), 3)
        if p > 0:
            nRet = int(s[0 : p])
            ls.append(self.__get_ascii2char(nRet))

        pTmp = p
        while pTmp < len(s):
            ls.append(self.__get_ascii2char(int(s[pTmp: pTmp + 3])))
            pTmp += 3
        return "".join(ls)

    @staticmethod
    def is_number(value):
        try:
            int(value)
            return True
        except (TypeError, ValueError):
            pass
        return False

    @staticmethod
    def is_ascii_range(n):
        return 0 <= n < 256

    @staticmethod
    def is_custom_ascii_range(n):
        return 33 <= n < 48 or 58 <= n < 126
