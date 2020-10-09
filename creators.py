#-- coding:utf-8 --

from lib.redis import Redis
from lib.mysql import Mysql
from lib.rabbitmq import Rabbitmq
import configparser
import os
import json
from threading import Thread
from time import sleep


#将redis中每个表的主键放到队列，每1000个存放一次。主键生产者
class Pri_key_creator():
    def __init__(self,RD,k,MQ,MYSQL):
        self.RD = RD
        self.k = k
        self.MYSQL = MYSQL
        self.MQ = MQ

    def __get_db_tables(self):
        res = self.RD.str_get(self.k)
        return json.loads(res)

    # 查出主键后传进队列
    def pri_key_to_queue(self,DB,TABLE,PRI):
        MTU = 1000  #主键切片大小
        select_pri_sql = 'select {0} from {1}.{2};'.format(PRI,DB,TABLE)
        conn = self.MYSQL.db_connect(DB)
        pri = self.MYSQL.db_select(conn,select_pri_sql)
        name = DB + '_' + TABLE
        self.MQ.declare(name)
        num = int(len(pri) / MTU)   #控制生成切片大小
        temp_pri = []       #临时存放主键
        for a in range(num + 1):
            startindex = a*MTU
            temp_pri = []           #初始化主键列表
            if a == num:            # 如果到了最后一轮，以防不足1000，最后索引就取总长度
                endindex = len(pri)
            else:
                endindex = (a+1)*MTU
            temp_pri = [ pri[i][0] for i in range(startindex,endindex) ]  #主键列表一次存储最多MTU个值
            return temp_pri


    def __multi_thread(self):
        db_table_columns_info = self.__get_db_tables()
        for db,tables in db_table_columns_info.items():
            DB = db
            threadlist = []
            for k,v in tables.items():
                TABLE = k
                PRI = v['prikey']
                t = Thread(target=self.pri_key_to_queue,args=(DB,TABLE,PRI))
                #self.pri_key_to_queue(DB,TABLE,PRI)
                sleep(0.1)
                threadlist.append(t)
                t.start()
            
            for thd in threadlist:
                thd.join()

    # def __multi_thread(self):
    #     self.__pri_key_to_queue('mysite','auth_group','id')

    def start(self):
        self.__multi_thread()
    


def main():
    #读取配置
    dir = os.path.dirname(os.path.abspath(__file__))    
    file = os.path.join(dir,'config.ini')
    config = configparser.ConfigParser()
    config.read(file,encoding='utf-8')
    try:
        mysql_src_item = dict(config.items('src_mysql'))
        mysql_dst_item = dict(config.items('dst_mysql'))
        redis_item = dict(config.items('redis'))
        mq_item = dict(config.items('rabbitmq'))
    except Exception as e:
        print(e)
        exit()

    # redis、源db、目标db、mq实例创建
    redisIns = Redis(host=redis_item['host'],
                        port=redis_item['port'],
                        password=redis_item['password'],
                        )

    src_db = Mysql(host=mysql_src_item['host'],
                    user=mysql_src_item['user'],
                    port=mysql_src_item['port'],
                    password=mysql_src_item['password'],
                    charactor=mysql_src_item['charactor'],
                    dblist=mysql_src_item['db'].split(',')
                    )

    dst_db = Mysql(host=mysql_dst_item['host'],
                    user=mysql_dst_item['user'],
                    port=mysql_dst_item['port'],
                    password=mysql_dst_item['password'],
                    charactor=mysql_dst_item['charactor'],
                    dblist=mysql_src_item['db'].split(',')                    
                    )

    mq = Rabbitmq(host=mq_item['host'],
                    port=mq_item['port'],
                    user=mq_item['user'],
                    password=mq_item['password']
                    )
     

    # 调用get_db_table_column_info方法，把库、表、表字段写进redis
    db_table_column_info = src_db.get_db_table_column_info(excludeTable=mysql_src_item['exclude'].split(','),
                                                            includeTable=mysql_src_item['include'].split(','))
    redisIns.str_set('db_table_column_info',db_table_column_info)

    # 生产者把主键存入队列
    creator = Pri_key_creator(redisIns,'db_table_column_info',mq,src_db)
    creator.start()
    exit()

    # 消费者
    

if __name__ == '__main__':
    main()