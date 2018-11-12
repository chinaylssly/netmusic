# -*- coding: utf-8 -*-

'分布式进程 -- 服务器端'

import random, multiprocessing,logging,time,sys
from multiprocessing.managers import BaseManager
from multiprocessing import freeze_support,Queue,Process
from mysql import Cloud_Music_MySQL
from mysqlpool import Cloud_Music_MySQL_Pool
import threading

filename=sys.argv[0].split('\\')[-1].split('.')[0]
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s - [line:%(lineno)s] - %(levelname)s :%(message)s',
                    filename='%s.log'%filename,
                    filemode='w'

                    )

song_info_queue =Queue()
song_comment_queue =Queue()

playlist_info_queue=Queue()
playlist_comment_queue=Queue()

##为queue绑定方法，否则会报method not bound error

def get_song_info_queue():
    return song_info_queue

def get_song_comment_queue():
    return song_comment_queue

def get_playlist_info_queue():
    return playlist_info_queue

def get_playlist_comment_queue():
    return playlist_comment_queue



BaseManager.register('get_song_info_queue',callable=get_song_info_queue)
BaseManager.register('get_song_comment_queue',callable=get_song_comment_queue)
BaseManager.register('get_playlist_info_queue',callable=get_playlist_info_queue)
BaseManager.register('get_playlist_comment_queue',callable=get_playlist_comment_queue)

manager=BaseManager(address=('127.0.0.1',5000),authkey='music')


def put_queue_model(query,queue,queue_name):

    mysql=Cloud_Music_MySQL()
    data=getattr(mysql,query)(limit=10000)
    mysql.close_connect()

    for d in data[1]:
        queue.put(d)
        logging.info(u'put %s into %s'%(d,queue_name))

    while not queue.empty():

        print u'[%s] current %s size:%s'%(time.asctime(),queue_name,queue.qsize())
        logging.info(u'check whether %s is empty after 10 second,current qsize is:%s'%(queue_name,queue.qsize()))
        time.sleep(10)

    logging.info(u'Queue empty , generate new queue')


def put_queue_model_special(parse,queue,queue_name):
    data=parse()
    for d in data:
        queue.put(d)
        logging.info(u'put %s into %s'%(d,queue_name))

    while not queue.empty():

        print u'[%s] current %s size:%s'%(time.asctime(),queue_name,queue.qsize())

        logging.info(u'check whether %s is empty after 10 second,current qsize is:%s'%(queue_name,queue.qsize()))
        time.sleep(10)
        
    logging.info(u'Queue empty , generate new queue')

def put_song_comment_queue_by_model():
    
    query='check_table_song'
    queue=manager.get_song_comment_queue()
    queue_name='song_comment_queue'

    put_queue_model(query=query,queue=queue,queue_name=queue_name)


def put_song_info_queue_by_model():

    mysql=Cloud_Music_MySQL()

    logging.info(u'更新playlist中status的值')
    mysql.auto_update_playlist_status()
    
    logging.info(u'获取playlist中status小于75的数据')
    data=mysql.check_table_playlist_status() 

    mysql.close_connect()

    
    queue=manager.get_song_info_queue()
    queue_name='song_info_queue'

    def parse():
        for i in data[1]:
            song_ids=i['song_ids'].split(',')
            refer=i['ids']
            for ids in song_ids:
                d=dict(ids=ids,refer=refer)
                yield d

    put_queue_model_special(parse=parse,queue=queue,queue_name=queue_name)

    
def put_playlist_info_queue_by_model():
    query='check_table_playlist_total'
    queue=manager.get_playlist_info_queue()
    queue_name='playlist_info_queue'
    put_queue_model(query=query,queue=queue,queue_name=queue_name)


def put_playlist_comment_queue_by_model():
    query='check_table_playlist'
    queue=manager.get_playlist_comment_queue()
    queue_name='playlist_comment_queue'
    put_queue_model(query=query,queue=queue,queue_name=queue_name)






if __name__ == '__main__':

    freeze_support()
    # 启动Queue:

    manager.start()

    t1=threading.Thread(target=put_song_info_queue_by_model)
    t2=threading.Thread(target=put_song_comment_queue_by_model)
    # t3=threading.Thread(target=put_playlist_info_queue_by_model)
    # t4=threading.Thread(target=put_playlist_comment_queue_by_model)

    t1.start()
    t2.start()

    # t3.start()
    # t4.start()
    
    t1.join()
    t2.join()

    # t3.join()
    # t4.join()

    print u'all task done,shutdown manager!!'

    manager.shutdown()