# _*_ coding:utf-8 _*_ 

from DBUtils.PersistentDB import PersistentDB
import MySQLdb
import MySQLdb.cursors
import logging

class persistentdb(object):
    def __init__(self,db='cloudmusic'):
        self.db=db
        self.Pool = PersistentDB(
                        creator = MySQLdb,  #使用链接数据库的模块
                        maxusage = None, #一个链接最多被使用的次数，None表示无限制
                        setsession = [], #开始会话前执行的命令
                        ping = 0, #ping MySQL服务端,检查服务是否可用
                        closeable = False, #conn.close()实际上被忽略，供下次使用，直到线程关闭，自动关闭链接，而等于True时，conn.close()真的被关闭
                        threadlocal = None, # 本线程独享值的对象，用于保存链接对象
                        host = '127.0.0.1',
                        port = 3306,
                        user = 'root',
                        passwd = '',
                        db = self.db,
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

        return count,data

    def close(self,):
        self.connection.close()

def test():
    p=persistentdb()
    sql='insert into test values(1,"test")'
    data=p.execute(sql)
    print data


def func_1(sql='select count(*) from song'):
    conn = PooL.connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    print(result)
    cursor.close()
    conn.close()##并不会关闭？？



def func():
    ids=random.randint(1,1000)
    sql='insert into test values(%s,"test")'%(ids,)
    mysql.execute(sql)


def test_pool_Thread():
    count=0
    while 1:
        t=threading.Thread(target=func)
        t.start()
        count+=1
        if count>100:
            print u'count =100,sleep 10 second'
            time.sleep(10)
            count=0



if __name__=='__main__':

    import threading
    for i in range(100):
        t = threading.Thread(target=test)
        t.start()

