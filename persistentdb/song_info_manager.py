# -*- coding: utf-8 -*-

'分布式进程 -- 服务器端'

import random, multiprocessing,logging,time
from multiprocessing.managers import BaseManager
from multiprocessing import freeze_support,Queue
from mysql import Cloud_Music_MySQL
from mysqlpool import Cloud_Music_MySQL_Pool


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s - [line:%(lineno)s] - %(levelname)s :%(message)s',
                    filename='song_info_manager.log',
                    filemode='w',
                    )

# mysql=Cloud_Music_MySQL_Pool()
##使用数据库连接池，persistentdb

# mysql=Cloud_Music_MySQL()
##非数据库连接池

song_info_queue =Queue()
song_comment_queue =Queue()

playlist_info_queue=Queue()
playlist_comment_queue=Queue()

def get_song_info_queue():
    return song_info_queue

def get_song_comment_queue():
    return song_comment_queue


BaseManager.register('get_song_info_queue',callable=get_song_info_queue)
BaseManager.register('get_song_comment_queue',callable=get_song_comment_queue)
manager=BaseManager(address=('127.0.0.1',5000),authkey='music')




def put_song_info_queue():

    
    mysql=Cloud_Music_MySQL()
    song_info_queue=manager.get_song_info_queue()

    def put_queue():
        
        logging.info(u'更新playlist set status=1 歌单中85%以上的歌曲被抓取可以将status更改为1')
        # print u'更新playlist set status=1 歌单中85%以上的歌曲被抓取可以将status更改为1'
        mysql.auto_update_playlist_status()
        
        logging.info(u'查询playlist中status小于75的歌单')
        # print u'查询playlist中status小于75的歌单'
        data=mysql.check_table_playlist_status()
        mysql.close_connect()
        # print data
        for i in data[1]:
            refer=i['ids']
            song_ids=i['song_ids'].split(',')
            for ids in song_ids:
                d=dict(ids=ids,refer=refer)
                song_info_queue.put(d)
                # print u'put dict into song_info_queue %s'%d
                logging.info(u'put dict into song_info_queue %s'%d)

    put_queue()
    
    while not song_info_queue.empty():

        print u'current queue size:%s'%(song_info_queue.qsize())

        logging.info(u'check whether queue is empty after 10 second,now queue is not empty,qsize is:%s'%song_info_queue.qsize())
        time.sleep(10)
        
    logging.info(u'Queue empty , generate new queue')
    
    # put_queue()
    



if __name__ == '__main__':

    freeze_support()
    # 启动Queue:
    manager.start()
    put_song_info_queue()
  