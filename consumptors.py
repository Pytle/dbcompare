#-- coding:utf-8 --
from lib.redis import Redis
from lib.mysql import Mysql
from lib.rabbitmq import Rabbitmq
import configparser
import os
import json
from multiprocessing import Process,Pool
import time
import queue

# 从队列中读取主键，redis中读取库、表和字段生成sql进行比较。结果回写到redis。主键消费者
class Pri_key_consumer():
    def __init__(self,RD,k,MQ,SRC_MYSQL,DST_MYSQL,QUEUE):
        self.RD = RD
        self.k = k
        self.MQ = MQ
        self.SRCMYSQL = SRC_MYSQL
        self.DSTMYSQL = DST_MYSQL
        self.QUEUE = QUEUE

    def __get_db_tables(self):
        res = self.RD.str_get(self.k)
        return json.loads(res)


    def comparetable(self,db,table,priname,colunms,pri):
        dir = os.path.dirname(os.path.abspath(__file__))
        file = os.path.join(dir,'result.log')
        diff_info = {}
        diff_info[db] = {}
        diff_info[db][table]={}
        colstr = '' 
        startpri = pri[0]
        endpri = pri[-1]
        # 拼接字段，生成sql语句
        for colunm in colunms:
            colstr = colstr + ',`{}`'.format(colunm)
        sql = 'select  concat_ws(\',\'{1})  from `{2}`.`{3}` where {4} > {5} and {4} < {6}'.format(priname,colstr,db,table,priname,startpri,endpri)
        src = self.SRCMYSQL.db_select(self.SRCMYSQL.db_connect(db),sql)[0][0]
        dst = self.DSTMYSQL.db_select(self.DSTMYSQL.db_connect(db),sql)[0][0]
        if src == dst:
            # with open(file,'a+') as f:
            #     f.write('diffprikey:{0}-{1}-{2},sql:{3}\n'.format(db,table,pri,sql))
            print('diffprikey:{0}-{1}-{2},sql:{3}\n'.format(db,table,pri,sql))         
        else:
            pass
        
    # 从MQ取回主键，存进本地队列
    def get_pri_from_mq(self,queuename):
        connection = self.MQ.get_connection()
        channel = connection.channel()
        #print(channel)
        for method_frame, properties, body in channel.consume(queuename,inactivity_timeout=3):
            try:
                pri = body.decode()
                channel.basic_ack(method_frame.delivery_tag)
                self.QUEUE.put((queuename,pri))
            except Exception as e:
                print("%s is over...." % queuename)
                break

    def dealwith_pri_key_from_mq(self):
        db_table_columns_info = self.__get_db_tables()
        info_tuple=()
        try:
            info_tuple = self.QUEUE.get(block=False, timeout=3)
            
        except Exception as e:
            print('queue is empty...')
        print(info_tuple)


    #多消费者
    def multi_pri_key_consumer(self):
        dir = os.path.dirname(os.path.abspath(__file__))
        file = os.path.join(dir,'result.log')
        with open(file,'w+') as f:
            f.truncate()                
        pool = Pool(processes=8)
        pool.apply_async(func=self.dealwith_pri_key_from_mq())
        pool.close()
        pool.join()

                




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

    # redis、源db、目标db实例创建
    redisIns = Redis(host=redis_item['host'],
                        port=redis_item['port'],
                        password=redis_item['password'],
                        )

    src_db = Mysql(host=mysql_src_item['host'],
                    user=mysql_src_item['user'],
                    port=mysql_src_item['port'],
                    password=mysql_src_item['password'],
                    charactor=mysql_src_item['charactor'],
                    dblist=mysql_src_item['db'].split(','),
                    )

    dst_db = Mysql(host=mysql_dst_item['host'],
                    user=mysql_dst_item['user'],
                    port=mysql_dst_item['port'],
                    password=mysql_dst_item['password'],
                    charactor=mysql_dst_item['charactor'],
                    dblist=mysql_src_item['db'].split(','),
                    )
                    
    mq = Rabbitmq(host=mq_item['host'],
                    port=mq_item['port'],
                    user=mq_item['user'],
                    password=mq_item['password'],
                    )

    q = queue.Queue()
    # 消费者
    consumer = Pri_key_consumer(redisIns,'db_table_column_info',mq,src_db,dst_db,q)
    
    Process(target=consumer.get_pri_from_mq, args='mysite_auth_permission').start()

    consumer.multi_pri_key_consumer()
    #print(Pri_key_consumer.__dict__)


if __name__ == '__main__':
    main()