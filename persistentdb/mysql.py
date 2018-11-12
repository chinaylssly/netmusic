
# _*_ coding:utf-8 _*_ 

from My_MySQL import MySQL,logging
# print id(logging)

class Cloud_Music_MySQL(MySQL):

    def __init__(self):

        super(Cloud_Music_MySQL,self).__init__(db='cloudmusic')
        # self.create_table_playlist()
        # self.create_table_song()

    def create_table_discover(self,):
        sql='''create table if not exists discover(
                ids bigint primary key,
                status int default "0",
                insert_time timestamp default current_timestamp)
                default charset utf8'''
        self.execute(sql)
        logging.info(u'create table discover successfully')

    def insert_table_discover(self,ids):
        sql='insert ignore into discover(ids) values(%s)'%(ids)

        try:
            self.execute(sql)
            logging.info(u'successfully insert ids to discover') 
        except Exception,e:
            logging.error(u'failed insert_table_playlist info cause by %s'%str(e)) 



    def create_table_playlist(self,):
 
        sql='''create table if not exists playlist(
           
                title varchar(200) not null default 'title',
                ids bigint  primary key not null ,
                author varchar(50),
                tags varchar(50),
                total int default '-1',
                fav int,
                comment int,
                share int,
                description varchar(5000),
                song_ids varchar(3000),
                comment_count int default '-1',
                comments text,
                status int default '0',
                insert_time timestamp default current_timestamp
                )default charset utf8
        '''
        
        self.execute(sql)
        logging.info(u'successfully create table playlist')



    def insert_table_playlist(self,title=0,ids=0,author=0,tags=0,total=0,fav=0,comment=0,share=0,description=0,song_ids=0):
        title=self.check_str(title)
        description=self.check_str(description)

        sql='''insert ignore into playlist (title,ids,author,tags,total,fav,comment,share,description,song_ids) 
                values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")'''%(title,ids,author,tags,total,fav,comment,share,description,song_ids)
        try:
            self.execute(sql)
            logging.info(u'successfully insert_table_playlist info') 
        except Exception,e:
            logging.error(u'failed insert_table_playlist info cause by %s'%str(e)) 
        
   

    def check_table_playlist_total(self,):##用于更新playlist info
        sql='select ids from playlist where total=-1  order by insert_time asc limit 1000'
        logging.info(u'正在查询数据表playlist中total为-1的数据')
        return self.execute(sql) 



    def check_table_playlist_status(self,status=75):##用于更新song
        '''检查抓取率小于75%的数据'''

        sql='select ids,song_ids from playlist where status <%s  order by insert_time asc limit 100 '%(status)
        logging.info(u'正在查询数据表playlist中status为0的数据')

        return self.execute(sql)  

    def check_table_playlist(self,):#用于更新playlist comments

        sql='select ids from playlist where comment_count=-1  order by insert_time asc limit 1000'
        logging.info(u'正在查询数据表playlist中comment_count为-1的数据')
        return self.execute(sql) 

    def insert_table_playlist_ids(self,ids):
        sql='insert ignore into playlist (ids) values(%s)'%(ids)
       
        try:
            self.execute(sql)
            logging.info(u'successfully insert_table_playlist_ds') 
        except Exception,e:
            logging.error(u'failed insert_table_playlist id cause by %s'%str(e)) 
        
   

    def update_table_playlist_info(self,title,ids,author,tags,total,fav,comment,share,description,song_ids):
        title=self.check_str(title)
        description=self.check_str(description)
  

        sql='''update playlist set title="%s",author="%s",tags="%s",total="%s",
                fav="%s",comment="%s",share="%s",description="%s",song_ids="%s" where ids=%s
                '''%(title,author,tags,total,fav,comment,share,description,song_ids,ids)

        try:
            self.execute(sql)
            logging.info(u'successfully update_table_playlist info') 
        except Exception,e:
            logging.error(u'failed update_table_playlist_info  cause by %s'%str(e)) 
        
   


    def update_table_playlist_comment(self,comment_count,comments,ids):
        '''可以与song合并成一个函数'''
        comments=self.check_str(comments)
        sql='''update playlist set comment_count="%s",comments="%s" where ids="%s "'''%(comment_count,comments,ids)
        try:
            self.execute(sql)
            logging.info(u'successfully update_table_playlist_comment info') 
        except Exception,e:
            logging.error(u'failed update_table_playlist_comment  cause by %s'%str(e)) 
        

    def update_table_playlist_status(self,ids,status):
        '''更改status为抓取百分率'''

        sql='update playlist set status=%s where ids=%s'%(status,ids)
        logging.info(u'更新playlist中status的值为%s'%(status))
        return self.execute(sql)

    def change_playlist_total(self,):##
        sql='select ids,song_ids from playlist where total=-1 order by insert_time asc'
        data=self.execute(sql)
        for i in data[1]:
            ids=i['ids']
            song_ids=i['song_ids']
            total=len(song_ids.split(','))
            print ids,total
            sql='update playlist set total=%s where ids=%s'%(total,ids)
            self.execute(sql)
            

    def auto_update_playlist_status(self,):

        sql='select count(*),refer from song group by refer'
        data=self.execute(sql)
        print data
        for i in data[1]:
            count=i['count(*)']
            refer=i['refer']
            sql='select total from playlist where ids=%s and status<75'%(refer)
            result=self.execute(sql)
            print result
            if result[0]:##
                total=result[1][0]['total']
                print refer,count,total

                status=int(count*1.0/total*100)

                assert status<101 ,'status max=100' ##抓取率最大为100
               
                self.update_table_playlist_status(ids=refer,status=status)
                logging.info(u'update playlist id=%s status=%s'%(refer,status))
               


                


    def create_table_song(self,):

        sql='''create table if not exists song(
                song varchar(50),
                singer varchar(50),
                ids bigint primary key not null,
                url varchar(200),
                refer bigint,
                comment_count int default '-1',
                comments text,
                status int default '0',
                insert_time timestamp default current_timestamp
                )default charset utf8
        '''
        logging.info(u'创建数据表song')
        self.execute(sql)


    def insert_table_song(self,song,singer,ids,url,refer):
        sql='''insert ignore into song(song,singer,ids,url,refer)
                values("%s","%s","%s","%s","%s")'''%(song,singer,ids,url,refer)
        
        try:
            self.execute(sql)
            logging.info(u'successfully insert_table_song') 
            
        except Exception,e:
            import traceback
            logging.error(u'traceback is:%s'%traceback.format_exc())
            print u'error cause by:',e
            logging.error(u'failed insert_table_song  cause by %s'%str(e)) 
        

    def update_table_song(self,comment_count,comments,ids):
        comments=self.check_str(comments)

        sql='''update song set comment_count="%s",comments="%s" where ids="%s"'''%(comment_count,comments,ids)
        try:
            self.execute(sql)
            logging.info(u'successfully update_table_song comments') 
        except Exception,e:
            logging.error(u'failed update_table_song comments cause by %s'%str(e)) 


    def check_table_song(self,limit=1000):#用于更新song comments
        sql='select ids from song where comment_count<0  order by  insert_time asc limit %s'%limit ##避免查询过多
        logging.info(u'检查数据表song中comment_count为-1的数据')
        return self.execute(sql)

    def check_table_song_ids(self,ids):#用于song ids中是否重复
        sql='select ids,comment_count from song where ids=%s'%(ids)
        info=u'检查歌曲信息是否已经插入'
        return mysql_execute(sql=sql,info=info)

    def mysql_execute(self,sql,info):
        logging.info(info)
        return self.execute(sql)

    def drop_table(self,tb='playlist'):
        sql='drop table %s'%tb
        logging.info(u'移除数据表%s'%(tb))
        self.execute(sql)

    def truncate_table(self,tb='song'):
        sql='drop table %s'%tb
        logging.info(u'truncate数据表%s'%(tb))
        self.execute(sql)

    def check_str(self,string):
        return string.replace('\\','\\\\').replace('"','\\"')

    def reset_playlist(self,):
        sql='update playlist set total=-1'
        self.execute(sql) 

    def close_connect(self,):
        self.close()

    def show_status(self,):
        sql='show status like "Thread%"'
        data=self.execute(sql)
        self.close_connect()
        return data

    def show_Threads_connected(self,):
        sql='show status like "Threads_connected"'
        data=self.execute(sql)
        # self.close_connect()
        return data[1][0]['Value']

    
    

def test():

    c=Cloud_Music_MySQL()
    # c.create_table_discover()
    # c.insert_table_discover(3)
    # c.create_table_playlist()
    # c.create_table_song()
    # c.drop_table()

    # c.insert_table_song('marry you','mars','12','www')
  
    # c.reset_playlist()
    # c.change_playlist_status()
    # c.auto_update_playlist_status()

    # sql='show variables like "Thread%"'
    def show_status():
        sql='show status like "Thread%"'
        data=c.execute(sql)
        for i in data[1]:
            print i['Variable_name'],i['Value']

    print c.show_Threads_connected()

    


if __name__ =='__main__':
    test()