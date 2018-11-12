# _*_ coding:utf-8 _*_ 
from discover_playlist import Discover_Playlist
from multi import Multi_Song_Info,Multi_Playlist_Info,Multi_Comment
from multiprocessing import Queue,Process
import multiprocessing,random,time,logging
from mysql import  Cloud_Music_MySQL
import os,sys,re,random,traceback
from concurrent.futures import ThreadPoolExecutor
import threading

'''
不写成类，需要函数中传递Queue()
写成类，则调用Pool非常不方便
多任务利用persistentdb 程序会崩溃
多进程调用类方法会提示类方法没有绑定，需要调用__call__函数，但这样的话就没必要写成类的形式了
'''

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    # filename='%s.log'%time.ctime().replace(' ','-').replace(':','#'),
                    filename='log.log',
                    filemode='w') 


class Cloud_Music(object):
   
    def __init__(self,):
        # self.mysql=Cloud_Music_MySQL()  ###所有mysql对象使用同一个mysql连接，会发生插入错误
        self.playlist_queue=Queue()
        self.table_playlist_info_queue=Queue()
        self.table_playlist_comment_queue=Queue()
        self.song_info_queue=Queue()
        self.song_comment_queue=Queue()
        self.mysql=Cloud_Music_MySQL()

        self.playlist_thread_pool=ThreadPoolExecutor(100)##专门用来执行更新playlist comment任务的线程，因为comment任务中有等待超时
        self.song_thread_pool=ThreadPoolExecutor(100) ##专门用来执行更新playlist comment任务的线程
        self.thread_pool=ThreadPoolExecutor(100) ##执行其余任务的线程

        # self.mysql=Cloud_Music_MySQL()

    def generate_playlist_ids_queue(self,):
        '''获取playlist ids'''

        logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
        print u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid())
        dp=Discover_Playlist()
        gen_dp=dp.main()
        for gen_ids in gen_dp:
            for ids in gen_ids:
                self.playlist_queue.put(ids)
                logging.info(u'put ids=%s into playlist_queue'%(ids))


    def insert_playlist_ids(self,):
        '''insert ids to playlist'''

        logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
        print u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid())
        
        while True:   
            try:
                # mysql=Cloud_Music_MySQL()###必须要线程池，否则开销太大
                ids=self.playlist_queue.get(block=True,timeout=30)
                self.thread_pool.submit(self.mysql.insert_table_playlist_ids,ids)
                logging.info(u'%s start new threading, get play ids=%s,threading name is:%s,pid is:%s'%(sys._getframe().f_code.co_name,ids,threading.current_thread().name,os.getpid()))
                   
            except Exception,e:
            
                if str(e):
                    logging.error(u'function %s raise  error cause by %s'%(sys._getframe().f_code.co_name,str(e)))
                else:##queue raise error e , str(e)为空
                    logging.warn(u' function %s raise  error cause by queue empty '%(sys._getframe().f_code.co_name))
                print e
                break
       


    def generate_table_playlist_info_queue(self,):
        '''select ids from playlist where total=-1 limit 1000'''

        logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
        print u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid())
        data=self.mysql.check_table_playlist_total()

        for i in data[1]:
            ids=i['ids']
            self.table_playlist_info_queue.put(ids)
            logging.info(u'put ids=%s into table_playlist_info_queue'%ids)
        # mysql.close_connect()
       


    def update_playlist_info(self,): ##定义函数，传递三个参数 queue(),target,args
        '''update playlist info'''

        logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
        print u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid())

        while True:
            try:  
                ids=self.table_playlist_info_queue.get(block=True,timeout=30)
                '''
                timeout=30,表示最多等待30s去queue中取数据,30S内取不到数据，表示generate'中没有数据可以put，
                将会抛出Empty异常，此时可以重新调用generate_queue函数。

                '''

                mpi=Multi_Playlist_Info(ids,self.mysql)
                self.thread_pool.submit(mpi.insert_table_playlist)
                logging.info(u'%s start new threading, get play ids=%s,threading name is:%s,pid is:%s'%(sys._getframe().f_code.co_name,ids,threading.current_thread().name,os.getpid()))
             

            except Exception,e:              
                if str(e):
                    e=str(e)
                else:##queue raise error e , str(e)为空
                    e='queue empty'
                logging.warn(u' function %s raise  error cause by %s,traceback info is:%s '%(sys._getframe().f_code.co_name,e,traceback.format_exc()))
                print e
                break

    def generate_table_playlist_comment_queque(self,):##可以传递function，queue
        '''select ids from playlist where comment_count=-1 limit=100'''

        logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
        print u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid())
        data=self.mysql.check_table_playlist()
        for i in data[1]:
            ids=i['ids']
            self.table_playlist_comment_queue.put(ids)
        # self.mysql.close_connect()



    def update_playlist_comment(self,):
        '''update playlist comment'''

        logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
        print u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid())
       
        while True:
            try:
                ids=self.table_playlist_comment_queue.get(block=True,timeout=30)
                mc=Multi_Comment(ids=ids,comment='playlist',thread_pool_executor=self.playlist_thread_pool,mysql=self.mysql)
                mc.get_first_comment()#获取第一页评论、total、page 
                mc.generate_page_queue() #获取page_queue
                mc.run_thread() #运行线程
                comments=mc.unique_comment()
                try:
                    mc.update_playlist_comment(comments=comments)
                except Exception,e:
                    logging.debug(u'playlist ids=%s update comment raise error:%s'%(ids,str(e)))

            except Exception,e:              
                if str(e):
                    e=str(e)
                else:##queue raise error e , str(e)为空
                    e='queue empty'
                logging.warn(u' function %s raise  error cause by %s,traceback info is:%s '%(sys._getframe().f_code.co_name,e,traceback.format_exc()))
                print e
                break


      


    def generate_song_info_queue(self,):
        '''select ids,song_ids from playlist where status=0'''

        logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
        print u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid())


        self.thread_pool.submit(self.mysql.auto_update_playlist_status)
        logging.info(u'start new threading to update playlist status')
        ##更新playlist set status=1 歌单中85%以上的歌曲被抓取可以将status更改为1

        data=self.mysql.check_table_playlist_status()

        for i in data[1]:
            refer=i['ids']
            song_ids=i['song_ids'].split(',')
            for ids in song_ids:
                d=dict(ids=ids,refer=refer)
                self.song_info_queue.put(d)
                logging.info(u'put dict into song info queue %s'%str(d))
        # self.mysql.close_connect()

    def insert_song_info(self,):

        logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
        print u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid())

        while True:

            try:   
                d=self.song_info_queue.get(block=True,timeout=30)  
                msi=Multi_Song_Info(d['ids'],d['refer'],self.mysql)
                self.thread_pool.submit(msi.insert_song)
                logging.info(u'%s start new threading, get song ids=%s,threading name is:%s,pid is:%s'%(sys._getframe().f_code.co_name,d['ids'],threading.current_thread().name,os.getpid()))
             
            except Exception,e:              
                if str(e):
                    e=str(e)
                else:##queue raise error e , str(e)为空
                    e='queue empty'
                logging.warn(u' function %s raise  error cause by %s,traceback info is:%s '%(sys._getframe().f_code.co_name,e,traceback.format_exc()))
                print e
                break

           

    def generate_song_comment_queque(self,p=10):
        '''select ids from song where comment_count=-1 limit'''

        logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
        print u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid())

        data=self.mysql.check_table_song()
        for i in data[1]:
            ids=i['ids']
            self.song_comment_queue.put(ids)
            logging.info(u'put %s into song_comment_queue'%ids)
       # self.mysql.close_connect()

    def update_song_comment(self,):#更新歌曲 评论

        logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
        print u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid())
        while True:

            try:
            
                ids=self.song_comment_queue.get(block=True,timeout=30)
                mc=Multi_Comment(ids=ids,comment='song',thread_pool_executor=self.song_thread_pool,mysql=self.mysql)
                mc.get_first_comment()#获取第一页评论、total、page 
                mc.generate_page_queue() #获取page_queue
                mc.run_thread() #运行线程
                comments=mc.unique_comment()
                try: 
                    mc.update_song_comment(comments=comments)
                except Exception,e:#捕捉数据库插入异常
                    logging.error(u'ids=%s的歌曲插入评论发生异常，异常原因：%s'%(ids,str(e)))

            except Exception,e:              
                if str(e):
                    e=str(e)
                else:##queue raise error e , str(e)为空
                    e='queue empty'
                logging.warn(u' function %s raise  error cause by %s,traceback info is:%s '%(sys._getframe().f_code.co_name,e,traceback.format_exc()))
                print e
                break

           

          




def test_Pool():
    '''Pool最好不要放入类方法,必须要放入的话要调用__call__()方法，python3无此bug'''


    logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
    cm=Cloud_Music()
    p=multiprocessing.Pool()

    s=\
    '''
    task1=Process(target=cm.generate_playlist_ids_queue,args=())
    task2=Process(target=cm.insert_playlist_ids,args=())
    task3=Process(target=cm.generate_table_playlist_info_queue,args=())
    task4=Process(target=cm.update_playlist_info,args=())
    task5=Process(target=cm.generate_table_playlist_comment_queque,args=())
    task6=Process(target=cm.update_playlist_comment,args=())
    task7=Process(target=cm.generate_song_info_queue,args=())
    task8=Process(target=cm.insert_song_info,args=())
    task9=Process(target=cm.generate_song_comment_queque,args=())
    task10=Process(target=cm.update_song_comment,args=())

    '''


    task=re.findall('cm\.(.*?),',s)
    # print getattr(cm,task[0])

    for i in range(10):
        func=getattr(cm,task[i])
        # print func
        p.apply_async(func,args=())

    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')

def step1():

    logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
    cm=Cloud_Music()
    thread_pool=cm.thread_pool
    thread_pool.submit(cm.generate_playlist_ids_queue)
    thread_pool.submit(cm.insert_playlist_ids)

def step2():

    logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
    cm=Cloud_Music()
    thread_pool=cm.thread_pool
    thread_pool.submit(cm.generate_table_playlist_info_queue)
    thread_pool.submit(cm.update_playlist_info)



def step3():

    logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
    cm=Cloud_Music()

    thread_pool=cm.thread_pool
    thread_pool.submit(cm.generate_table_playlist_comment_queque)
    thread_pool.submit(cm.update_playlist_comment)

  

def step4():
    logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
    cm=Cloud_Music()
    thread_pool=cm.thread_pool
    thread_pool.submit(cm.generate_song_info_queue)
    thread_pool.submit(cm.insert_song_info)




def step5():
    logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
    cm=Cloud_Music()
    thread_pool=cm.thread_pool
    thread_pool.submit(cm.generate_song_comment_queque)
    thread_pool.submit(cm.update_song_comment)




def test():

    logging.info(u'%s process runing pid:%s'%(sys._getframe().f_code.co_name,os.getpid()))
    cm=Cloud_Music()
    thread_pool=cm.thread_pool
    song_thread_pool=cm.song_thread_pool

    # thread_pool.submit(cm.generate_playlist_ids_queue)
    # thread_pool.submit(cm.insert_playlist_ids)

    # thread_pool.submit(cm.generate_table_playlist_info_queue)
    # thread_pool.submit(cm.update_playlist_info)

    # thread_pool.submit(cm.generate_table_playlist_comment_queque)
    # thread_pool.submit(cm.update_playlist_comment)

    # thread_pool.submit(cm.generate_song_info_queue)
    # thread_pool.submit(cm.insert_song_info)

    thread_pool.submit(cm.generate_song_comment_queque)
    song_thread_pool.submit(cm.update_song_comment)
    song_thread_pool.submit(cm.update_song_comment)
    # song_thread_pool.submit(cm.update_song_comment)
    # song_thread_pool.submit(cm.update_song_comment)





def run():
    P1=Process(target=step4,args=())
    P2=Process(target=step5,args=())
    P1.start()
    P2.start()
    P1.join()
    P2.join()
                

       

if __name__ =='__main__':

    # multiprocessing.freeze_support()
    # step3()
    # step2()
    # step4()
    # step5()
    # step1()

  
    test()
    # run()

    pass