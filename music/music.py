# _*_ coding:utf-8 _*_ 

import json
import requests
import time
from params import  Params

class Comment(Params):
    def __init__(self,ids=2238914673,category='comment',comment='playlist'):
        '''初始化Comment类，并继承Parama中__init__方法
            根据category确定是评论还是url
            根据model确定是音乐还是歌单
            category:[url,comment]
            comment:[playlist,song]
            url:[]

            也可以直接将该类定义在Params类中， 不需要继承

        '''
     
        super(Comment,self).__init__(ids=ids,category=category)###用以更写父类的属性
        # self.ids=ids
        # self.category=category  ##父类中已经有了属性的定义，不需要再次定义了
        # Params.__init__(self)  ##继承父类


        d=dict(playlist='A_PL_0_',song='R_SO_4_')
        self.extend=d[comment]+str(self.ids)

        print self.extend 

        if category=='comment':
            self.url='http://music.163.com/weapi/v1/resource/comments/%s'%self.extend
        elif category=='url':
            self.url='http://music.163.com/weapi/song/enhance/player/url'
        else:
            self.url=None
            assert self.url



    def get_response(self,page=1,per=20,br=128000):
        '''获取commment response'''

        data=dict(
            params=self.aes_encrpt(page=page,per=per,br=br),
            encSecKey=self.rsa_encrpt()
            )

        return requests.post(url=self.url,params=self.url_params,data=data,headers=self.headers,timeout=30)

    def get_page(self,per=20):
        '''获取评论总页码'''

        response=self.get_response(per=per)
        self.first_comment=response ###直接赋予新的属性，而不返回值
    
        res=response.json()
        total=int(res['total'])
        # print u'total is %s:'%(total)

        if total%per==0:
            return total/per

        return int(total/per)+1


class Song(Params):
    '''获取歌曲信息'''
    def __init__(self,ids=460043704,category='url'):
        super(Song,self).__init__(ids=ids,category=category)
        self.url='http://music.163.com/weapi/song/enhance/player/url'

    def get_response(self,):
        '''获取commment response'''

        data=dict(
            params=self.aes_encrpt(),
            encSecKey=self.rsa_encrpt()
            )

        return requests.post(url=self.url,params=self.url_params,data=data,headers=self.headers,timeout=30)

        

def get_comments(ids=2238914673,per=20,comment='playlist'):

    c=Comment(ids=ids,comment=comment)
    page=c.get_page(per=per)
    response=c.first_comment

    show_comments(response=response)

    for page in range(2,page+1):
        response=c.get_response(per=per,page=page)
        show_comments(response)
        break

def show_comments(response):
    
    content=response.json()
    comments=content['comments']
    for comment in comments:
        d=dict(
                        content=comment['content'].strip(),
                        nickname=comment['user']['nickname'],
                        userid=comment['user']['userId'],
                        )

        
        j=json.dumps(d,ensure_ascii=False)

        print j
    
    print u'sleep 0.5 s'
    print '\n'
    time.sleep(0.5)

def get_url(ids=475597835,br=128000):
    c=Comment(ids=ids,category='url')
    response=c.get_response(br=br)
    print response.json()
    return  response.json()



if __name__ =='__main__':

   
    # get_url()
    get_comments(ids=2250054577)
    # get_comments()
    get_comments(ids=475597835,comment='song')


    pass
