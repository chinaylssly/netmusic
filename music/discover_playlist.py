# _*_ coding:utf-8 _*_ 
from My_requests import My_requests
import time
from mysql import Cloud_Music_MySQL
import logging

class Discover_Playlist(My_requests):
    """docstring for Discover_Playlist"""
    def __init__(self,):

        super(Discover_Playlist, self).__init__()

        self.params=dict(
                        order='hot',
                        cat='全部',
                        limit=35,    
                        offset=35,
                        )

        self.url='http://music.163.com/discover/playlist'
        self.soup=self.get_soup()
        self.mysql=Cloud_Music_MySQL()

    def callback(self,offset,):
        time.sleep(1)
        print u'sleep 1 s'

        self.params=dict(
                        order='hot',
                        cat='全部',
                        limit=35,    
                        offset=offset,
                        )
        self.soup=self.get_soup()

            


    def parse_ids(self,): 
        p=self.soup.find_all('p',attrs={'class':'dec'})
        for pp in p:
            try:
                ids=pp.a['href'].split('=')[-1]
                print u'获取歌单ids：%s'%ids
                yield ids
            except Exception,e:
                logging.error(u'parse_ids error cause by %s'%sr(e))   

    def parse_next(self,):

        a=self.soup.find('a',attrs={'class':"zbtn znxt"})
        try:
            offset=a['href'].rsplit('=',1)[-1]
            logging.info(u'successfully get new offset=%s'%(offset))
        except Exception,e:
            logging.error(u'no more next page,so will break loop cause by %s'%str(e))
            offset=None
            
        return offset

    def main(self,):

        yield self.parse_ids()     
        offset=self.parse_next()
        # count=1
        while offset:
            # count+=1
            # if count>3:
            #     break          
            self.callback(offset)
            yield self.parse_ids()     
            offset=self.parse_next()


            

        




        





def test():
    dp=Discover_Playlist()
    print dp.url
    print dp.headers
    print dp.method

    gen_dp=dp.main()
    for gen_ids in gen_dp:

        for ids in gen_ids:
            print ids 


    # dp.parse_ids()
    # dp.parse_next()



if __name__ =='__main__':
    test()

    pass
    
        