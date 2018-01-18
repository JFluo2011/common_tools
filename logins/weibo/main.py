import re
import time
import json
import base64
import binascii

import rsa
import requests

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
}


class WeiBo(object):
    def __init__(self, user_name, password):
        self.user_name = user_name
        self.password = password
        self.session = requests.Session()

    def pre_login(self):
        pre_time = str(int(time.time() * 1000))
        url = 'https://login.sina.com.cn/sso/prelogin.php'
        params = {
            'entry': 'weibo',
            'callback': 'sinaSSOController.preloginCallBack',
            'su': base64.b64encode(self.user_name.encode('utf-8')).decode('utf-8'),
            'rsakt': 'mod',
            'checkpin': '1',
            'client': 'ssologin.js(v1.4.19)',
            '_': pre_time,
        }
        r = self.session.get(url, params=params, headers=HEADERS)
        return pre_time, json.loads(r.text[r.text.find('(') + 1:r.text.rfind(')')])

    def login(self, pre_time, pre_login_data):
        pubkey = pre_login_data['pubkey']
        exectime = str(pre_login_data['exectime'])
        rsa_pubkey = int(pubkey, 16)
        key = rsa.PublicKey(rsa_pubkey, 65537)
        nonce = pre_login_data['nonce']
        server_time = pre_login_data['servertime']
        # self.set_public(pubkey, '10001')
        message = str(server_time) + '\t' + nonce + "\n" + self.password
        sp = binascii.b2a_hex(rsa.encrypt(message.encode(encoding="utf-8"), key))
        prelt = str(int(time.time()) - int(pre_time) - int(exectime, 16) | 0)
        url = 'https://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.19)'
        form_data = {
            'entry': 'weibo',
            'gateway': '1',
            'from': '',
            'savestate': '7',
            'qrcode_flag': 'false',
            'useticket': '1',
            'pagerefer': ('https://login.sina.com.cn/crossdomain2.php?'
                          'action=logout&r=https%3A%2F%2Fweibo.com%2Flogout.php%3Fbackurl%3D%252F'),
            'vsnf': '1',
            'su': base64.b64encode(self.user_name.encode('utf-8')).decode('utf-8'),
            'service': 'miniblog',
            'servertime': str(server_time),
            'nonce': nonce,
            'pwencode': 'rsa2',
            'rsakv': pre_login_data['rsakv'],
            'sp': sp,
            'sr': '1680*1050',
            'encoding': 'UTF-8',
            'prelt': prelt,
            'url': 'https://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack',
            'returntype': 'META',
        }
        r = self.session.post(url, data=form_data, headers=HEADERS)
        url = re.findall(r'location.replace\("(.*?)"\)', r.text)[0]
        r = self.session.get(url)
        url = re.findall(r'location.replace\(\'(.*?)\'\)', r.text)[0]
        r = self.session.get(url)
        uid = re.findall('"uniqueid":"(\d+)",', r.text)[0]
        url = 'https://weibo.com/u/{}'.format(uid)
        print(url)
        r = self.session.get(url)
        return 'XXXX' in r.text  # nickname or something else in the page


def main():
    user_name = 'user_name'
    password = 'password'
    weibo = WeiBo(user_name, password)
    pre_time, pre_login_data = weibo.pre_login()
    if weibo.login(pre_time, pre_login_data):
        print('登录成功！')


if __name__ == '__main__':
    main()
