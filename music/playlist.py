# _*_ coding:utf-8 _*_ 

from My_requests import My_requests
import json
import logging

class Playlist(My_requests):
    '''
    继承My_requests类，并重写url参数
    '''
    def __init__(self,ids=2238914673):

        self.ids=ids
        self.url='http://music.163.com/playlist?id=%s'%(str(self.ids))
        super(Playlist,self).__init__(url=self.url)
        self.soup=self.get_soup()

        logging.info(u' playlist request ids is:%s'%(self.ids))

   
    def get_song_list(self):
        
        ul=self.soup.find('ul',attrs={'class':'f-hide'})
        for li in ul:
            a=li.a
            d=dict(
                ids=a['href'].split('=')[-1],
                name=a.get_text().strip(),
            )

            j=json.dumps(d,ensure_ascii=False)
            print j
            yield d

    def get_song_ids(self,):
        ul=self.soup.find('ul',attrs={'class':'f-hide'})
        song_ids=[]
        for li in ul:
            ids=li.a['href'].split('=')[-1]
            song_ids.append(ids)
        logging.info(u'get song_ids from playlist=%s'%self.ids)
        return ','.join(song_ids)




    def get_playlist_info(self,):

        share_a=self.soup.find('a',attrs={'class':'u-btni u-btni-share '})
        try:
            description=self.soup.find('p',attrs={'id':'album-desc-more'}).get_text().strip()

        except Exception,e: ##出现空 description
            logging.error(u'failed get description of playlist=%s,cause by %s'%(self.ids,str(e)))
            description=''

        try:
            tags=self.soup.find('div',attrs={'class':'tags f-cb'}).get_text().split(u'：')[-1].strip().replace('\n',',')
        except:
            logging.error(u'failed get tags of playlist=%s,cause by %s'%(self.ids,str(e)))
            tags=''
        logging.info(u'get playlist=%s info'%self.ids)
        return dict(
        fav=self.soup.find('a',attrs={'class':'u-btni u-btni-fav '})['data-count'],
        comment=self.soup.find('span',attrs={'id':'cnt_comment_count'}).get_text(),
        share=share_a['data-count'],
        author=share_a['data-res-author'],
        title=share_a['data-res-name'],
        description=description,
        tags=tags,
        total=self.soup.find('span',attrs={'id':'playlist-track-count'}).get_text(),

        )







def main():
    instance=Playlist()
    data=instance.get_song_list()
    data.next()
    soup=instance.soup

    fav=soup.find('a',attrs={'class':'u-btni u-btni-fav '})['data-count']
    comment=soup.find('span',attrs={'id':'cnt_comment_count'}).get_text()
    share_a=soup.find('a',attrs={'class':'u-btni u-btni-share '})
    share=share_a['data-count']
    author=share_a['data-res-author']
    title=share_a['data-res-name']
    description=soup.find('p',attrs={'id':'album-desc-more'}).get_text().strip()
    tags=soup.find('div',attrs={'class':'tags f-cb'}).get_text().split(u'：')[-1].strip().replace('\n',',')
    total=soup.find('span',attrs={'id':'playlist-track-count'}).get_text()
        

    print fav,comment,share,title,author,tags,total
    print description
   

if __name__ =='__main__':
    main()