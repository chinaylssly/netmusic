# _*_ coding:utf-8 _*_ 

from My_requests import My_requests
import json
import logging

class Song(My_requests):
    '''
    继承My_requests类，并重写url参数
    '''
    def __init__(self,ids=4049385):

        self.ids=ids
        self.url='http://music.163.com/song?id=%s'%(str(self.ids))
        super(Song,self).__init__(url=self.url)

        # print u'Song class ids is:%s(default 4049385)'%self.ids


    def get_song_info(self,):

        a=self.get_soup().find('a',attrs={'class':'u-btni u-btni-share '})     
        return dict(
        song=a['data-res-name'],
        singer=a['data-res-author'],
        )
        

        



def test():
    s=Song()
    print s.get_song_info()


if __name__ =='__main__':

    test()