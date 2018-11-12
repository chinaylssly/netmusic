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


song_comment_queue =Queue()

##为queue绑定方法，否则会报method not bound error

def get_song_comment_queue():
    return song_comment_queue

BaseManager.register('get_song_comment_queue',callable=get_song_comment_queue)
manager=BaseManager(address=('127.0.0.1',5000),authkey='music')



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
    
    data=Cloud_Music_MySQL().check_table_song(limit=5000)
    queue=manager.get_song_comment_queue()
    def parse_data():
        for i in data[1]:
            ids=i['ids']
            yield dict(ids=ids)

    put_queue_model(parse_data=parse_data,queue=queue)











if __name__ == '__main__':

    freeze_support()
    # 启动Queue:
    manager.start()
  
    put_song_comment_queue_by_model()
  