# _*_ coding:utf-8 _*_

from model_worker import model_run,model_test,manager
from multi import Multi_Song_Info
import logging



def song_info_worker():
    queue=manager.get_song_info_queue()
    def func(d):
        msi=Multi_Song_Info(song_ids=d['ids'],refer=d['refer'])
        return msi.insert_song()#传递结果，不能传递函数对象，不加括号，表示返回的是函数对象，需要真正调用msi.insert_song需要以func(d=d)()的形式调用

    model_run(queue=queue,func=func)



song_info_worker()