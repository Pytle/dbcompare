#-- coding:utf-8 --
from lib.mysql import Mysql
import configparser
import os
import time
import json
from tasks import test,compare
from sqlutils import SRCMYSQL,DSTMYSQL,db_table_column_info,redisins


def taskstart(src_db,DB,TABLE,PRI,colunms,log,errlog):
    MTU = 1000  #主键切片大小
    select_pri_sql = 'select {0} from {1}.{2};'.format(PRI,DB,TABLE)
    conn = src_db.db_connect(DB)
    pri = src_db.db_select(conn,select_pri_sql)
    num = int(len(pri) / MTU)   #控制生成切片大小
    temp_pri = []       #临时存放主键
    
    # 删除key
    rd = redisins.getins()
    errkeyname = "error-{0}-{1}".format(DB,TABLE)
    if rd.keys(errkeyname):
        rd.delete(*rd.keys(errkeyname))
    okkeyname = "ok-{0}-{1}".format(DB,TABLE)
    if rd.keys(okkeyname):
        rd.delete(*rd.keys(okkeyname))

        
    for a in range(num + 1):
        startindex = a*MTU
        temp_pri = []           #初始化主键列表
        if a == num:            # 如果到了最后一轮，以防不足1000，最后索引就取总长度
            endindex =  len(pri)
        else:
            endindex = (a+1)*MTU 
        temp_pri = [ pri[i][0] for i in range(startindex,endindex) ]  #主键列表一次存储最多MTU个值                  
        tid = compare.delay(DB,TABLE,PRI,colunms,temp_pri)
    if tid.get():
        print("finish.")
            
def main():
    # 初始化日志目录
    basedir = os.path.dirname(os.path.abspath(__file__))
    logdir = os.path.join(basedir,'log')
    if not os.path.isdir(logdir):
        os.mkdir(logdir)
        
    # 定义日志名称
    logdir = os.path.join(basedir,'log')
    thistime = time.strftime("%Y%m%d%H%M%S", time.localtime())
    logname = "result_" + thistime + ".log"
    errname = "err_" + thistime + ".log"
    log = os.path.join(logdir,logname)
    errlog = os.path.join(logdir,errname)

    
    # 消费者    
    for db,tables in db_table_column_info.items():
        DB = db
        rd = redisins.getins()
        for k,v in tables.items():
            TABLE = k
            PRI = v['prikey']
            colunms = v['columns']
            taskstart(SRCMYSQL,DB,TABLE,PRI,colunms,log,errlog)
            
            errkeyname = "error-{0}-{1}".format(DB,TABLE)
            print("errinfo:{0}".format(rd.lrange(errkeyname,0,1)))
            okkeyname = "ok-{0}-{1}".format(DB,TABLE)
            print("okinfo:{0}\n".format(rd.lrange(okkeyname,0,1)))
            
                

if __name__ == '__main__':
    main()
