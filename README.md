# netmusic

网易云音乐基于multiprocessing的分布式爬虫

1、music与persistentdb的区别在于后者使用了mysql连接池，前者未使用

2、song comment manager用来分发抓取歌曲的评论的任务，song comment worker 用来执行任务。

3、song info manager用来分发抓取歌曲相关信息以及url的任务，song info worker 用来执行任务。

4、本爬虫对歌曲的抓取是基于歌单的！

