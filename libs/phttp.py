# -*- coding: utf-8 -*-

import urllib
import urllib2
import random
import hashlib
import json
import libs.qcloud.sms.SmsSender as SmsSender
'''
import cookielib
import base64
import re
import json
import hashlib
'''
#mail
import smtplib
from email.mime.text import MIMEText
# from email.MIMEText import MIMEText
import sys
reload(sys)
sys.setdefaultencoding('utf8')

from config import config


# http 操作类
class phttp:

    # 初始化配置信息
    def __init__(self):
        pass

    @staticmethod
    def file_get_contents(url='', getstatus=False, data=None, headers=None, timeout=30):
        if not url:
            return False
        status = False
        try:
            headers = headers if headers else {'User-Agent': 'GA2.0'}
            req = urllib2.Request(url=url, data=data, headers=headers)
            up = urllib2.urlopen(req, timeout=timeout)
            contents = up.read()
            up.close()
            status = True
        except urllib2.HTTPError, e:
            contents = e
        except urllib2.URLError, e:
            contents = e
        except Exception, e:
            contents = e
        if getstatus:
            return contents, status
        else:
            return contents
    #_encode
    @staticmethod
    def _encode(content='', tocode='utf-8', fromcode='utf-8'):
        if isinstance(content, unicode):
            content = content.encode(tocode)
        elif tocode != fromcode:
            content = content.decode(fromcode).encode(tocode)
        return content

    #urlencode
    def url_encode(self, dicts={}):
        for key in dicts:
            dicts[key] = self._encode(dicts[key])
        r = urllib.urlencode(dicts)
        
        return r

    #mail for python
    @staticmethod
    def send_mail(mails, realname='', subject='', content=''):
        if not isinstance(mails, type(['a'])):
            mails = mails.split(',')
        #设置服务器，用户名、口令以及邮箱的后缀
        mail_host = config.mail_config["host"]
        mail_user = config.mail_config["user"]
        mail_pass = config.mail_config["password"]
        
        me = config.mail_config["name"]+"<"+mail_user+">"
        msg = MIMEText(content)
        msg['Subject'] = subject
        msg['From'] = me
        msg['To'] = "%s<%s>" % (realname, ','.join(mails))
        try:
            s = smtplib.SMTP()
            s.connect(mail_host)
            s.login(mail_user, mail_pass)
            s.sendmail(me, mails, msg.as_string())
            s.close()
            return True
        except Exception, e:
            print str(e)
            return False

    #sms 短信发送
    def send_sms(self, phones, message='', method='qcloud'):
        if method == 'qcloud':
            return self.qcloud_sms(phones, message)
        else:
            return self.old_sms(phones, message)

    def old_sms(self, phones, message=''):

        if isinstance(phones, type(['a'])):
            phones = ",".join(phones)
        param = dict()
        param['sdk'] = config.sms_config["sdk"]
        param['code'] = config.sms_config["code"]
        param['subcode'] = config.sms_config["subcode"]
        param['phones'] = phones
        param['msg'] = message

        params = self.url_encode(param)
        url = 'http://vip.4001185185.com/sdk/smssdk!mt.action?%s' % params
        r = self.file_get_contents(url)
        #print r
        return r

    def qcloud_sms(self, phones, message=''):
        if not phones:
            return 'phone is null'
        if isinstance(phones, type('')):
            phones = phones.split(",")


        # 请根据实际 appid 和 appkey 进行开发，以下只作为演示 sdk 使用
        appid = 1400012451
        appkey = "5449848a8f6528ce28fcd44e15684d32"
        templ_id = 9028

        multi_sender = SmsSender.SmsMultiSender(appid, appkey)

        params = [message]
        result = multi_sender.send_with_param("86", phones, templ_id, params, "", "", "")
        return result