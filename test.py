#-- coding:utf-8 --

from lib.mysql import Mysql
import configparser
import os
import json
from tasks import test,compare



def taskstart(src_db,dst_db,DB,TABLE,PRI,colunms):
    MTU = 1000  #主键切片大小
    select_pri_sql = 'select {0} from {1}.{2};'.format(PRI,DB,TABLE)
    conn = src_db.db_connect(DB)
    pri = src_db.db_select(conn,select_pri_sql)
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
        compare.delay(DB,TABLE,PRI,colunms,temp_pri,src_db,dst_db)
            
def main():
    #读取配置
    basedir = os.path.dirname(os.path.abspath(__file__))    
    file = os.path.join(basedir,'config.ini')
    config = configparser.ConfigParser()
    config.read(file,encoding='utf-8')
    try:
        mysql_src_item = dict(config.items('src_mysql'))
        mysql_dst_item = dict(config.items('dst_mysql'))
    except Exception as e:
        print(e)
        exit()

    # 初始化日志目录
    logdir = os.path.join(basedir,'log')
    if not os.path.isdir(logdir):
        os.mkdir(logdir)
        
    # 源db、目标db实例创建
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

                                                            
    db_table_column_info = json.loads(db_table_column_info)
    
    # 消费者
    
    for db,tables in db_table_column_info.items():
        DB = db
        for k,v in tables.items():
            TABLE = k
            PRI = v['prikey']
            colunms = v['columns']
            taskstart(src_db,dst_db,DB,TABLE,PRI,colunms)
            
                

if __name__ == '__main__':
    main()
