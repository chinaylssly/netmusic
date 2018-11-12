# _*_ coding:utf-8 _*_
""" a work manager sample """

from multiprocessing.managers import BaseManager
import time,logging,threading
from concurrent.futures import ThreadPoolExecutor
import traceback,sys,os
from mysql import Cloud_Music_MySQL
from multi import Multi_Song_Info,Multi_Comment
from mysqlpool import Cloud_Music_MySQL_Pool
import threading

filename=sys.argv[0].split('\\')[-1].split('.')[0]
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s - [line:%(lineno)s] - %(levelname)s :%(message)s',
                    filename='%s.log'%filename,
                    filemode='w'

                    )
max_pool=11
##线程池大小
pool=ThreadPoolExecutor(max_pool)

g_multiple=3
##同时创建线程数相较于max_pool的倍数，设置2到3最好


# 从网络上获取Queue
# BaseManager.register('get_song_info_queue')

# 连接服务器
server_addr='127.0.0.1'
print 'Connect to server %s ...' % server_addr


manager = BaseManager(address=('127.0.0.1', 5000), authkey='music')
manager.connect()


def model_run(queue,func):##需要传入两个参数，queue为manage中管理的queue，func为需要提交的线程任务,需要带参数
    '''主函数'''

    avg_time=model_test(queue=queue,func=func)##平均一个线程花费的时间
    logging.info(u'from function test get avg_time=%s'%avg_time)
    print u'from function test get avg_time=%s'%avg_time
    count=0
    start_time=time.time()
    multiple=g_multiple
    
    while True:
        try:
            logging.info(u'max wait 100 second try to get element from queue')
            # d=song_info_queue.get(timeout=100)
            d=queue.get(timeout=100)

            logging.info(u'get %s from queue'%d)

            # msi=Multi_Song_Info(song_ids=d['ids'],refer=d['refer'])
            # pool.submit(msi.insert_song)

            pool.submit(func,d)

            logging.info(u'%s start new threading, get song ids=%s,threading name is:%s,pid is:%s'%(sys._getframe().f_code.co_name,d['ids'],threading.current_thread().name,os.getpid()))
            count+=1

            if count>=max_pool*multiple:
                sleeptime=multiple*avg_time
                print u'generate %s threading,so sleep %s second! current active threading num=%s'%(count,sleeptime,threading.active_count())
                time.sleep(sleeptime)
                count=0

                end_time=time.time()
                if end_time - start_time>=600:##每过10十分做一次检测，确保数据库连接数最好介于30到150之间，保证程序稳定运行

                    mysql=Cloud_Music_MySQL()
                    Threads_connected=mysql.show_Threads_connected()
                    mysql.close_connect()

                    if Threads_connected<=30:   ##说明程序运行效率不高，可以适当提高multiple，或是降低avg_time
                        avg_time=test()
                        multiple=multiple+1
                        info=u'程序闲置过多，current Threads_connected=%s,重设avg_time=%s,multiple=%s'%(Threads_connected,avg_time,multiple)
                        print info
                        logging.info(info)


                    elif Threads_connected>=150: ##说明程序负荷过重，可以适当降低multiple，或是提高avg_time
                        avg_time=test()
                        multiple=max(multiple-1,2)##multiple最小为2
                        info=u'程序负荷过重，current Threads_connected=%s,重设avg_time=%s,multiple=%s'%(Threads_connected,avg_time,multiple)
                        print info
                        logging.info(info)

                    else:
                        info=u'程序运行良好，current Threads_connected=%s,保持avg_time=%s,multiple=%s'%(Threads_connected,avg_time,multiple)
                        print info
                        logging.info(info)

                    start_time=time.time()##重设start_time



         

        except Exception,e:              
            if str(e):
                e=str(e)
            else:##queue raise error e , str(e)为空
                e='queue empty'

            logging.warn(u' function %s raise  error cause by %s,traceback info is:%s '%(sys._getframe().f_code.co_name,e,traceback.format_exc()))
            print u'error info is:%s'%e

            if 'many connections' in e:##最好使用joinablequeue,##经过600秒一次的性能检测，很难抛出too many connections 异常了
                print u'current too many connections,sleep 3 second wait runing connections close'
                # song_info_queue.put(d)
                queue.put(d)
                print u'catch too many connections error ,so put d=%s back into queue'%d
                logging.info(u'catch too many connections error ,so put d=%s back into queue'%d)

                ##发生异常在于数据库操作，d的值可以获取到，所以把他重新放回queue中，所以不需要joinablequeue了

                mysql=Cloud_Music_MySQL()
                Threads_connected=mysql.show_Threads_connected()
         
                while Threads_connected>=100: 
                    info=u'current Threads_connected is:%s,also too much,so sleep 3 second!'%Threads_connected
                    print info
                    logging.debug(info)
                    time.sleep(3)
                    Threads_connected=mysql.show_Threads_connected()
                mysql.close_connect()
                continue

            elif 'empty' in e:
                print u'empty queue,break loop!'
                print u'wait 20 second ensure runing threading done'
                time.sleep(20)
                break

            else:
                info=u'unexcept error,here is traceback info:%s'%(traceback.format_exc())
                print info
                logging.error(info)
                # song_info_queue.put(d)
                queue.put(d)
                print u'catch unexcept error ,so put d=%s back into queue'%d
                break




def model_test(queue,func):##用于测试网络环境

    info=u'runing %s to test avg_time'%(sys._getframe().f_code.co_name)
    print info
    logging.info(info)
    multiple=g_multiple
    count=0
    t1=time.time()
    results=[]
    times=[]

    while True:

        logging.info(u'wait 100 second try to get element from queue')
        # d=song_info_queue.get(timeout=100)
        d=queue.get(timeout=100)
        logging.info(u'get %s from queue'%d)

        # msi=Multi_Song_Info(song_ids=d['ids'],refer=d['refer'])
        # result=pool.submit(msi.insert_song)

        result=pool.submit(func,d)

        # song_info_queue.put(d)## 不需要放回去了，因为已经更新了！！
        results.append(result)
        logging.info(u'%s start new threading, get song ids=%s,threading name is:%s,pid is:%s'%(sys._getframe().f_code.co_name,d['ids'],threading.current_thread().name,os.getpid()))
        count+=1
        if count>=max_pool*multiple:
            print u'generate 60 threading to test avg_time'
            for i in results:
                t=i.result()##该方法是阻塞的
                times.append(t)
            
            print times
            t2=time.time()
            print u'test cost time:%s'%(t2-t1)
            # return times,t2-t1
            print u'test finish！'
            return sum(times)/len(times)
            




def song_info_worker():
    queue=song_info_queue
    def func(d):
        msi=Multi_Song_Info(song_ids=d['ids'],refer=d['refer'])
        return msi.insert_song()#传递结果，不能传递函数对象，不加括号，表示返回的是函数对象，需要真正调用msi.insert_song需要以func(d=d)()的形式调用

    model_run(queue=queue,func=func)






if __name__=='__main__':
    
    song_info_worker()  
    
    # run()        
    
    pass