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

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s - [line:%(lineno)s] - %(levelname)s :%(message)s',
                    filename='worker.log',
                    filemode='w'
                    )
max_pool=11
pool=ThreadPoolExecutor(max_pool)

##io密集型任务，线程数=x*N+1 N为cpu核数，x最好为2到5之间的整数

##使用persistentdb程序会崩溃，该方法会将queue中数据全部读完，崩溃的原因可能是mysql连接过多
##难道是一个线程只能使用 该线程中数据库pool()

# mysql=Cloud_Music_MySQL_Pool()

# mysql=Cloud_Music_MySQL()
##不能只用一个mysql对象进行mysql插入操作，会抛出mysql have gone away 异常

# 从网络上获取Queue
BaseManager.register('get_song_info_queue')

# 连接服务器
server_addr='127.0.0.1'
print 'Connect to server %s ...' % server_addr


manager = BaseManager(address=('127.0.0.1', 5000), authkey='music')
manager.connect()

# 获取Queue对象
song_info_queue = manager.get_song_info_queue()

def run():
    count=0
    multiple=3
    while True:
        try:
            logging.info(u'wait 100 second try to get element from queue')
            d=song_info_queue.get(timeout=100)
            logging.info(u'get %s from queue'%d)
            # mysql=Cloud_Music_MySQL()
            '''
            不使用连接池技术可能会造成mysql连接开销过大,如果对产生的线程数不加限制的话，使用此方法从queue中读取到的数据接近于mysql的max_connections，
            不使用数据库连接池会导致mysql status中thread数暴涨，很快到达瓶颈(网络不好的情况下？？)
            '''


            # mysql=Cloud_Music_MySQL_Pool()
            ##使用数据库连接池和不使用目前还没有看出差别

            msi=Multi_Song_Info(song_ids=d['ids'],refer=d['refer'])
            pool.submit(msi.insert_song)##
            # mysql.close()
            #不能在此关闭数据库连接，因为主线程非阻塞，子线程数据库相关任务还没有完成，主线程就把数据库连接给关闭了
              
            logging.info(u'%s start new threading, get song ids=%s,threading name is:%s,pid is:%s'%(sys._getframe().f_code.co_name,d['ids'],threading.current_thread().name,os.getpid()))
            count+=1
            if count>max_pool*multiple:##大于一定值线程数需要等待线程处理，不要回报too many connections。程序线程数大于22，一般在46或是67左右，可能是由于一个线程在
            ##运行结束后没有关闭数据库连接，一定时间内，系统还为其保留数据库连接，导致线程处于闲置状态。（因为其没有任务可以接受）。
                print u'generate 60 threading,so sleep 20 second! current active threading num=%s'%threading.active_count()
                sleeptime=multiple*6
                time.sleep(sleeptime)
                count=0
            '''
            系统每创建60个线程就会休息60s，因为线程池的大小为21个，所以同时会运行21个线程，假如每个线程运行时间差不多，每个线程运行时间需要平均3s，
            那么大概9秒的时间，60个线程就会全部运行完毕。所以大概等待9秒左右，能够最大程度的利用系统资源。避免了过多等待时间。
            sleeptime=count最大值/线程池大小*平均每个线程运行时间(sleeptime=60/21*3=9)
            网络环境良好的环境下，数据库线程数很小（宿舍测试max_pool=11,count=22,timesleep=20的情况下，数据库线程数稳定在22）
            网络良好的情况下，sleeptime=30，max_count=60,max_pool=21时，mysql status线程数稳定在30-70左右。(在)
            网络良好的情况下，sleeptime=10，max_count=60,max_pool=21时，mysql status线程数越跑越多。运行5分钟后，线程会跑到500左右
            
            线程平均运行时间最大的影响因素在于网络流畅程度。网络状态良好的话，sleeptime设置20是没问题的。
            如果主线程不sleep的话，创建很多等待的线程，这些线程不能利用到连接池的优势，即一个数据库连接可以被不同的线程利用多次。
            因为数据库连接池中的每一个连接都被等待中的线程占用了。

            '''

        except Exception,e:              
            if str(e):
                e=str(e)
            else:##queue raise error e , str(e)为空
                e='queue empty'

            logging.warn(u' function %s raise  error cause by %s,traceback info is:%s '%(sys._getframe().f_code.co_name,e,traceback.format_exc()))
            print u'error info is:%s'%e

            if 'many connections' in e:##最好使用joinablequeue
                print u'current too many connections,sleep 3 second wait runing connections close'
                song_info_queue.put(d)
                print u'catch too many connections error ,so put d=%s back into queue'%d
                logging.info(u'catch too many connections error ,so put d=%s back into queue'%d)
                ##发生异常在于数据库操作，d的值可以获取到，所以把他重新放回queue中，所以不需要joinablequeue了
                time.sleep(3)
                continue
            else:
                print u'empty queue or other unknown error,so break loop!'
                print u'wait 20 second ensure runing threading done'
                time.sleep(20)
                break




def test():

    count=0
    while 1:
        try:
            d=song_info_queue.get(timeout=5)
            logging.info(u'get %s from queue'%d)
            
            
            pool.submit(func,d)
            count+=1
            if count>60:
                print u'count=60,休息6秒'
                time.sleep(6)
                count=0
        except:
            traceback.print_exc()


run()