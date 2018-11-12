# _*_ coding:utf-8 _*_ 
from Queue import Queue
import logging
from concurrent.futures import ThreadPoolExecutor
import threading,time,random,sys

from mysql import Cloud_Music_MySQL

class pool():

    def __init__(self,):

        self.max_connect=11
        self.run_connect=Queue()
        for i in range(self.max_connect):
  
            self.run_connect.put(Cloud_Music_MySQL())

        # print self.run_connect.qsize()


    def put_queue(self,element):
        self.run_connect.put(element)


    def get_queue_size(self,):
        return self.run_connect.qsize()


    def get_queue(self,timeout=3):

        while True:
            try: 
                return self.run_connect.get(timeout=timeout)
            except:
                print u'cant get element from queue'##每三秒轮训一次，查看有没有可用的的数据库连接


p=pool()
pp=ThreadPoolExecutor(13)

def test():
    sql='show status like "%threads_connected%"'
    mysql=p.get_queue()
    print u'id is:%s'%(id(mysql))
    data=mysql.execute(sql)
    print u'queue size is:%s'%(p.get_queue_size())
    
   
    # print u'connect_num is:%s'%(data[1][0]['Value'])
    print u'sleep 4 second'
    time.sleep(4)

    p.put_queue(mysql)##函数执行完把连接返回线程池


for i in range(12):
    pp.submit(test) 
    # t=threading.Thread(target=test)
    # t.start()
    '''
    线程池大小=11，连接池大小=11，range=12时，
    使用线程池submit方法是,当test执行时间大于get的timeout时间时，也不会打印出cant get element 异常。
    而不使用线程池，当test执行时间大于get的timeout时间时，就会打印出cant get element 异常，
    猜测：线程池有数量大小限制，线程池刚好等于数据库连接池大小，刚好剩下一个线程，剩下的一个线程只是在等待，并没有提交从连接池中获取mysql连接的任务。

    线程池大小=13，连接池大小=13，range=12时，
    两种方式均能捕捉到异常


    假设:线程池大小为thread，连接池大小为mysql，range为range,且函数运行时间大于timeout时间
    如果thread>mysql,且range>mysql时。会捕捉到异常；
    如果thread>mysql,且range<mysql时。将捕捉不到异常；
    如果thread<mysql,无论range取值多大，都捕捉不到异常。


    '''

print p.get_queue_size()