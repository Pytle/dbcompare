from lib.mysql import Mysql
import configparser
import os
import json

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
    
SRCMYSQL = Mysql(host=mysql_src_item['host'],
                user=mysql_src_item['user'],
                port=mysql_src_item['port'],
                password=mysql_src_item['password'],
                charactor=mysql_src_item['charactor'],
                dblist=mysql_src_item['db'].split(','),
                )

DSTMYSQL = Mysql(host=mysql_dst_item['host'],
                user=mysql_dst_item['user'],
                port=mysql_dst_item['port'],
                password=mysql_dst_item['password'],
                charactor=mysql_dst_item['charactor'],
                dblist=mysql_src_item['db'].split(','),
                )
                
#初始化数据
db_table_column_info = SRCMYSQL.get_db_table_column_info(excludeTable=mysql_src_item['exclude'].split(','),
                                                        includeTable=mysql_src_item['include'].split(','))

                                                        
db_table_column_info = json.loads(db_table_column_info)




