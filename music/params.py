# _*_ coding:utf-8 _*_ 
import json
from Crypto.Cipher import AES
import base64
from binascii import hexlify

class Params(object):
    def __init__(self,ids=2238914673,category=None):
        
        self.e ='010001'
        self.f='00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7'
        self.g='0CoJUm6Qyw8W8jud'
        self.i='a'*16
        self.ids=ids
        self.category=category
        self.url_params={'csrf_token':''}
        self.headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36'}


    def get_comment_j5o(self,page,per=20):
        '''获取评论的j5o参数'''
        # j5o:{rid: "A_PL_0_2164695804", offset: "0", total: "true", limit: "20", csrf_token: ""} 歌单comment的j5o参数

        j5o={
            # 'rid':'A_PL_0_'+str(self.ids),
            'offset':int(page-1)*per,
            # 'total':'true',
            'limit':per,
            # 'csrf_token':''
             }
         ###传入json数据按不按位置无所谓的,参数rid、total 以及csfr_token可以不要 
        print u'post data is:',j5o
        return json.dumps(j5o)

    def get_url_j5o(self,br=128000):
        '''获取音乐url的参数'''

        j5o={
            'ids': [self.ids], 
            'br': br,
            'csrf_token':'',
            }

        print u'post data is:',j5o
        return json.dumps(j5o)
        ###csrf_token参数可以忽略

    def aes_encrypto(self,text,key):
        '''AES加密算法'''

        iv="0102030405060708"
        pad=16-len(text)%16
        text=text+pad*chr(pad)
        encryptor=AES.new(key,AES.MODE_CBC,iv)
        result=encryptor.encrypt(text)
        result_str=base64.b64encode(result)
        return result_str


    def aes_encrpt(self,page=1,per=20,br=128000):
        '''获取params参数'''

        if self.category =='comment':
            self.d=self.get_comment_j5o(page=page,per=per)
        elif self.category=='url':
            self.d=self.get_url_j5o(br=br)
        else:
            self.d=None
        assert self.d ##吐过d不存在，则报错 

        result_str=self.aes_encrypto(self.d,self.g)  
        result_str=self.aes_encrypto(result_str,self.i)
        # print u'params is:',result_str    
        return result_str

    def rsa_encrpt(self,):
        '''获取encSecKey参数'''

        i=self.i[::-1]#翻转字符串
        rs = pow(int(hexlify(i), 16), int(self.e, 16), int(self.f, 16))
        #pow(x,y,z) 相当于x**y%z
        #int(hex_num,16)将16进制数转化成10进制数
        #hexlify(将字符串转化成16进制数字表示 需要utf-8编码，网页默认编码为utf-8

        return format(rs, 'x').zfill(256)
        #format(num,'x')将num转化为16进制字母小写
        #str.zfill(256) 将字符串填充为256位，不够则在字符串左边填充0


