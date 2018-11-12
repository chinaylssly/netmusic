# -*- coding: utf-8 -*-

'分布式进程 -- 服务器端'

import random, multiprocessing,logging,time,sys
from multiprocessing.managers import BaseManager
from multiprocessing import freeze_support,Queue
from mysql import Cloud_Music_MySQL
from mysqlpool import Cloud_Music_MySQL_Pool

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


BaseManager.register('get_song_info_queue',callable=get_song_info_queue)
BaseManager.register('get_song_comment_queue',callable=get_song_comment_queue)
manager=BaseManager(address=('127.0.0.1',5000),authkey='music')




def put_song_info_queue():

    
    mysql=Cloud_Music_MySQL()


    def put_queue():
        song_info_queue=manager.get_song_info_queue()
        # logging.info(u'更新playlist set status=1 歌单中85%以上的歌曲被抓取可以将status更改为1')
        # print u'更新playlist set status=1 歌单中85%以上的歌曲被抓取可以将status更改为1'
        # mysql.auto_update_playlist_status()
        
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

        print u'[%s] current queue size:%s'%(time.asctime(),song_info_queue.qsize())

        logging.info(u'check whether queue is empty after 10 second,current qsize is:%s'%(song_info_queue.qsize()))
        time.sleep(10)
        
    logging.info(u'Queue empty , generate new queue')
    
    # put_queue()
    

def put_song_comment_queue():
   
    mysql=Cloud_Music_MySQL()
    def put_queue():
        song_comment_queue=manager.get_song_comment_queue()
        
        data=mysql.check_table_song()
        mysql.close_connect()
        for i in data[1]:
            ids=i['ids']
            song_comment_queue.put(ids)
            logging.info(u'put %s into song_comment_queue'%ids)
    put_queue()
    while not song_comment_queue.empty():

        print u'[%s] current queue size:%s'%(time.asctime(),song_comment_queue.qsize())

        logging.info(u'check whether queue is empty after 10 second,current qsize is:%s'%(song_comment_queue.qsize()))
        time.sleep(10)
        
    logging.info(u'Queue empty , generate new queue')
    


def put_queue_model(parse_data,queue):

    for i in parse_data():
        queue.put(i)
        logging.info(u'put %s into queue'%(i))

    while not queue.empty():

        print u'[%s] current queue size:%s'%(time.asctime(),queue.qsize())

        logging.info(u'check whether queue is empty after 10 second,current qsize is:%s'%(queue.qsize()))
        time.sleep(10)

        
    logging.info(u'Queue empty , generate new queue')




def put_song_comment_queue_by_model():
    
    data=Cloud_Music_MySQL().check_table_song()
    queue=manager.get_song_comment_queue()
    def parse_data():
        for i in data[1]:
            ids=i['ids']
            yield ids

    put_queue_model(parse_data=parse_data,queue=queue)


def put_song_info_queue_by_model():

    mysql=Cloud_Music_MySQL()

    logging.info(u'更新playlist中status的值')
    mysql.auto_update_playlist_status()

    print u'获取playlist中status小于75的数据'
    data=mysql.check_table_playlist_status()

    mysql.close_connect()

    queue=manager=get_song_info_queue()

    def parse_data():
        for i in data[1]:
            song_ids=i['song_ids'].split(',')
            refer=i['ids']
            for ids in song_ids:
                d=dict(ids=ids,refer=refer)
                yield d

    put_queue_model(parse_data=parse_data,queue=queue)

    








if __name__ == '__main__':

    freeze_support()
    # 启动Queue:
    manager.start()
    # put_song_info_queue()
    # put_song_comment_queue()

    # put_song_info_queue_by_model()
    put_song_comment_queue_by_model()
  