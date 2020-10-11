# coding:utf-8
from celery import Celery
import os
import json
import time
import configparser
from sqlutils import SRCMYSQL,DSTMYSQL

#读取配置
dir = os.path.dirname(os.path.abspath(__file__))
file = os.path.join(dir,'config.ini')
config = configparser.ConfigParser()
config.read(file,encoding='utf-8')
try:
    redis_item = dict(config.items('redis'))
except Exception as e:
    print(e)
    exit()

app = Celery('producer',
                broker='redis://:{0}@{1}:{2}/0'.format(redis_item['password'],redis_item['host'],redis_item['port']),
                backend='redis://:{0}@{1}:{2}/0'.format(redis_item['password'],redis_item['host'],redis_item['port']),              
            )

class test():
	@app.task
	def test(x,y):
    		time.sleep(3)
    		return x + y
            
def selector(db,table,priname,colstr,pri,startpri,endpri,errlog):
    sql = 'select  concat_ws(\',\'{1})  from `{2}`.`{3}` where {4} >= \'{5}\' and {4} <= \'{6}\''.format(priname,colstr,db,table,priname,startpri,endpri)
    try:
        srcinfo = SRCMYSQL.db_select(SRCMYSQL.db_connect(db),sql)
        dstinfo = DSTMYSQL.db_select(DSTMYSQL.db_connect(db),sql)
        src = srcinfo[0][0]
        dst = dstinfo[0][0]
        return src,dst,sql
    except Exception as e:
        info = "select err info:{0}, content:{1} , sql:{2} ,pri:{3} - {4}\n".format(e,srcinfo,sql,startpri,endpri)
        with open(errlog,'a+') as f1:
            f1.write(info)
        return 255,255,255  
    
        
@app.task
def compare(db,table,priname,colunms,pri,log,errlog):
    if not pri:
        return 0
    diff_info = {}
    diff_info[db] = {}
    diff_info[db][table]={}
    colstr = '' 
    startpri = pri[0]
    endpri = pri[-1]
    # 拼接字段，生成sql语句
    for colunm in colunms:
        colstr = colstr + ',`{}`'.format(colunm)
    
    src,dst,sql = selector(db,table,priname,colstr,pri,startpri,endpri,errlog)
    if src == 255 and dst == 255 and sql == 255:
        return 255
    
    if src == dst:
        with open(log,'a+') as f:
            f.write('ok:{0}-{1}-{2},sql:{3}\n'.format(db,table,startpri,sql))
        return 0
    else:
        # 二分法查找不一致的数据
        
        with open(errlog,'a+') as f1:
            f1.write("{0}-{1}-{2} is not match\n".format(db,table,startpri + "-" + endpri))
        return 1
        
            
