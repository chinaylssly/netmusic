# _*_ coding:utf-8 _*_ 

from multiprocessing import Queue
import multiprocessing
import time
from cloud_music import Cloud_Music,get_comments,get_url
import json
from playlist import Playlist
from song import Song
from mysql import Cloud_Music_MySQL
import threading
import random,logging
from concurrent.futures import ThreadPoolExecutor
import traceback,sys
from mysqlpool import Cloud_Music_MySQL_Pool

from persistentdb import persistentdb  
'''
多任务分发
评论多任务分发
歌曲信息多任务分发

'''

class Multi_Song_Info(Cloud_Music):
    ##不能多继承 重名了get_response()函数

    def __init__(self,song_ids=518725853,refer=2147483647):##2147483647为32位操作系统int的最大值 即2^31-1

        super(Multi_Song_Info,self).__init__(ids=song_ids,category='url')
        self.refer=refer
        self.mysql=Cloud_Music_MySQL()

        

    def get_info(self,):

        s=Song(ids=self.ids)
        info=s.get_song_info()

        self.song=info['song']
        self.singer=info['singer']
        logging.debug(u'id=%s song is:%s,singer is:%s'%(self.ids,self.song,self.singer))


    def get_url(self,):

        response=self.get_response().json()
        self.url=response['data'][0]['url']
        logging.debug(u'the url of song id=%s is:%s'%(self.ids,self.url))
 
    def insert_song(self,):

        t1=time.time()
      
        self.get_info()
        # self.get_url() url会失效，不获取了
        self.url=''
        try:
            self.mysql.insert_table_song(song=self.song,singer=self.singer,url=self.url,ids=self.ids,refer=self.refer)
            ##已经有了try except 语句，所有线程一定会结束
        except:
            traceback.print_exc()
        finally:
            self.mysql.close_connect()
        t2=time.time()

        logging.info(u'try to close mysql connect avoid to many connect')
        print u'线程执行完毕！一共花费%s秒，关闭数据库连接'%(t2-t1)
        logging.debug(u'finish insert_song threading,total cost time is:%s'%(t2-t1))
        ##需要关闭连接，不然的话，线程就不能即时结束,会产生过多数据库连接。使用连接池会忽略close.connect，因而不会发生mysql server has gone away
        return t2-t1

def test_Multi_Song_Info(ids=31654455):

    ms=Multi_Song_Info(song_ids=ids)
    
    ms.get_info()
    ms.get_url()

    print ms.ids
    print ms.song
    print ms.singer
    print ms.url


class Multi_Playlist_Info(Playlist):

    def __init__(self,ids=2190625773,mysql=Cloud_Music_MySQL()):
        super(Multi_Playlist_Info,self).__init__(ids=ids)
        self.mysql=mysql##persistentdb 线程池


    def insert_table_playlist(self,):

        playlist_dict=self.get_playlist_info()
        song_ids=self.get_song_ids()

        self.mysql.update_table_playlist_info(fav=playlist_dict['fav'],comment=playlist_dict['comment'],share=playlist_dict['share'],
            author=playlist_dict['author'],title=playlist_dict['title'],description=playlist_dict['description'],
            tags=playlist_dict['tags'],total=playlist_dict['total'],song_ids=song_ids,ids=self.ids)

        

def test_Multi_Playlist_Info(ids=2197936899):
    mpi=Multi_Playlist_Info(ids=ids)
    playlist_dict=mpi.get_playlist_info()
    song_ids=mpi.get_song_ids()
    print playlist_dict['title']
    print song_ids

    



class Multi_Comment(Cloud_Music):
    '''利用进程池和线程池'''
    def __init__(self,ids=518725853,comment='song',):

        super(Multi_Comment,self).__init__(ids=ids,category='comment',comment=comment)
     
        self.comments=[]
        self.mysql=Cloud_Music_MySQL()
        self.comment=comment

    def parse_comments(self,response):
        comment_json=''
        content=response.json()
        comments=content['comments']
        for comment in comments:
            d=dict(
                    content=comment['content'].strip(),
                    nickname=comment['user']['nickname'],
                    userid=comment['user']['userId'],
                    likedcount=comment['likedCount'],
                    time=comment['time'],
                     )

            j=json.dumps(d,ensure_ascii=False)
  

            comment_json+=j+'\n'
        
        return comment_json
  
    def get_first_comment(self,):
        '''通过绑定属性，将会获得三个返回值，self.page,self.total'''

        self.page=self.get_page()
        response=self.first_comment
        logging.info(u'ids 为%s的歌曲，评论总页码为：%s'%(self.ids,self.page))
        comment=self.parse_comments(response)
        self.comments.append(comment)
        

    def get_other_comment(self,page=2):

        # logging.info(u'正在抓取ids=%s第%s页评论'%(self.ids,page))
        response=self.get_response(page=page)
        comment=self.parse_comments(response)
        logging.info(u'正在抓取ids=%s第%s页评论'%(self.ids,page))
        self.comments.append(comment)

    def get_all_comment(self,max_page=5):
        self.min_page=min(max_page,self.page)##最多抓取页数，避免抓取过多
        for page in range(2,self.min_page+1):
            self.get_other_comment(page=page)
            logging.info(u'获取ids=%s的%s的第%s页评论'%(self.ids,self.comment,page))


    def unique_comment(self,):
        info=u'得到%s页评论中的%s页评论，准备合并评论'%(self.min_page,len(self.comments))
        print info
        logging.info(info)
        return '\n'.join(self.comments)



    def update_song_comment(self,comments):
        # mysql=Cloud_Music_MySQL()   ##可以做成线程池
        self.mysql.update_table_song(comments=comments,comment_count=self.total,ids=self.ids)
        self.mysql.close_connect()

    def update_playlist_comment(self,comments):

        # mysql=Cloud_Music_MySQL()   ##可以做成线程池
        self.mysql.update_table_playlist_comment(comments=comments,comment_count=self.total,ids=self.ids)
        self.mysql.close_connect()






def test_comment():

    mc=Multi_Comment(ids=139774,comment='song')
    mc.get_first_comment()#获取第一页评论、total、page 
    mc.generate_page_queue(min_page=10) #获取page_queue
    mc.run_thread() #运行线程
    comments=mc.unique_comment() 
    print comments
    # mc.update_playlist_comment(comments=comments)










    
if __name__ =='__main__':

    # test_Multi_Song_Info(ids=440353010)
    test_comment()

    # test_Multi_Playlist_Info()


    pass