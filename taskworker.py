#!/usr/bin/env python
# _*_ coding:utf-8 _*_
""" a work manager sample """
import Queue
from multiprocessing.managers import BaseManager

import time

__author__ = 'Devin -- http://zhangchuzhao.site'


class QueueManager(BaseManager):
    pass

# 从网络上获取Queue
QueueManager.register('get_task_queue')
QueueManager.register('get_result_queue')

# 连接服务器
server_addr = '127.0.0.1'
print 'Connect to server %s ...' % server_addr
manager = QueueManager(address=(server_addr, 5000), authkey='abc')
manager.connect()

# 获取Queue对象
task = manager.get_task_queue()
result = manager.get_result_queue()

# 从task队列取任务，并把结果写入result队列
for i in range(10):
    try:
        n = task.get(timeout=1)
        print 'run task %d * %d ...' % (n, n)
        r = '%d * %d = %d' % (n, n, n * n)
        time.sleep(1)
        result.put(r)
    except Queue.Empty:
        print 'task quue is empty'

print 'worker exit!'