#-- coding:utf-8 --

from lib.mysql import Mysql
from lib.redis import Redis
from lib.rabbitmq import Rabbitmq
import configparser
import os
import json
from time import sleep
from tasks import test,Pri_key_consumer


def taskstart(MYSQL,DB,TABLE,PRI,colunms,consumer):
    MTU = 1000  #主键切片大小
    select_pri_sql = 'select {0} from {1}.{2};'.format(PRI,DB,TABLE)
    conn = MYSQL.db_connect(DB)
    pri = MYSQL.db_select(conn,select_pri_sql)
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
        consumer.comparetable.delay(DB,TABLE,PRI,colunms,temp_pri)
            
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
                    
                    
    
    #初始化数据
    db_table_column_info = src_db.get_db_table_column_info(excludeTable=mysql_src_item['exclude'].split(','),
                                                            includeTable=mysql_src_item['include'].split(','))
    #redisIns.str_set('db_table_column_info',db_table_column_info)


    # 消费者
    consumer = Pri_key_consumer(redisIns,'db_table_column_info',src_db,dst_db)
    #consumer.comparetable.delay(db,table,priname,colunms,pri)
    
    for db,tables in db_table_column_info.items():
        DB = db
        for k,v in tables.items():
            TABLE = k
            PRI = v['prikey']
            colunms = v['colunms']
            taskstart(src_db,DB,TABLE,PRI,colunms,consumer)
            
                

if __name__ == '__main__':
    main()
