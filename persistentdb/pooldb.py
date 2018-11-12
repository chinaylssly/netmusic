# _*_ coding:utf-8 _*_ 

import time
import MySQLdb
import MySQLdb.cursors
import threading
from DBUtils.PooledDB import PooledDB,SharedDBConnection
import logging

class pooldb(object):
    def __init__(self,):

        self.Pool = PooledDB(
                        creator = MySQLdb, #使用链接数据库的模块
                        maxconnections = 0,  #连接池允许的最大连接数，0和None表示没有限制
                        mincached = 5, #初始化时，连接池至少创建的空闲的连接，0表示不创建
                        maxcached = 0, #连接池空闲的最多连接数，0和None表示没有限制
                        maxshared = 0, #连接池中最多共享的连接数量，0和None表示全部共享，ps:其实并没有什么用，因为pymsql和MySQLDB等模块中的threadsafety都为1，所有值无论设置多少，_maxcahed永远为0，所以永远是所有链接共享
                        blocking = True, #链接池中如果没有可用共享连接后，是否阻塞等待，True表示等待，False表示不等待然后报错
                        setsession = [],#开始会话前执行的命令列表
                        ping = 0,#ping Mysql 服务端，检查服务是否可用
                        host = '127.0.0.1',
                        port = 3306,
                        user = 'root',
                        passwd = '',
                        db = 'cloudmusic',
                        charset = 'utf8',
                        cursorclass=MySQLdb.cursors.DictCursor,
                    )
        self.connection=self.Pool.connection()
        self.cursor=self.connection.cursor()

    def execute(self,sql):

        count=self.cursor.execute(sql)
        data=self.cursor.fetchall()
        self.connection.commit()
        logging.info(u'fetchall count is：%s'%count)
        # logging.info(u'查询结果为：%s'%str(data))
        self.connection.close()
        return count,data

    def close(self,):
        self.connection.close()

def test():
    p=pooldb()
    sql='insert into test values(1,"test")'
    data=p.execute(sql)
    print data

def func():
    #检测当前正在运行的连接数是否小于最大的连接数，如果不小于则等待连接或者抛出raise TooManyConnections异常
    #否则优先去初始化时创建的连接中获取连接SteadyDBConnection
    #然后将SteadyDBConnection对象封装到PooledDedicatedDBConnection中并返回
    #如果最开始创建的连接没有链接，则去创建SteadyDBConnection对象，再封装到PooledDedicatedDBConnection中并返回
    #一旦关闭链接后，连接就返回到连接池让后续线程继续使用
    conn = POOL.connection()
    cursor = conn.cursor()
    cursor.execute('select count(*) from song')
    result = cursor.fetchall()
    print(result)
    conn.close()


import threading
for i in range(100):
    t = threading.Thread(target=test)
    t.start()
###100个会报too many connection错误
