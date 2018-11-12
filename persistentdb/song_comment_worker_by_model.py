# _*_ coding:utf-8 _*_

from model_worker import model_run,model_test,manager,pool,BaseManager
from multi import Multi_Comment
import time,logging


BaseManager.register('get_song_comment_queue')


def func(d):

    t1=time.time()
    ids=d['ids']
    mc=Multi_Comment(ids=ids,comment='song')
    mc.get_first_comment()#获取第一页评论、total、page 
    mc.get_all_comment()
    comments=mc.unique_comment()
    try: 
        mc.update_song_comment(comments=comments)
    except Exception,e:#捕捉数据库插入异常
        logging.error(u'ids=%s的歌曲插入评论发生异常，异常原因：%s'%(ids,str(e)))
    t2=time.time()
    return t2-t1


def song_comment_worker():
    queue=manager.get_song_comment_queue()
    model_run(queue=queue,func=func)



song_comment_worker()