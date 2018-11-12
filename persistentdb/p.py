# _*_ coding:utf-8 _*_

from persistentdb import  persistentdb
from concurrent.futures import ThreadPoolExecutor
import random,time,os,sys,threading
from mysql import Cloud_Music_MySQL
import sys,os,logging,traceback

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s - [line:%(lineno)s] - %(levelname)s :%(message)s',
                    # filename='test.log',
                    )



p=3



def func():

    pp=p
    pp=pp-1
    print pp
    

func()
print p