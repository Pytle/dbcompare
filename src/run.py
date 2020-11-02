#-- coding:utf-8 --
from lib.mysql import Mysql
import configparser
import os
import time
import json
from tasks import test,compare
from sqlutils import SRCMYSQL,DSTMYSQL,db_table_column_info,redisins


def taskstart(src_db,DB,TABLE,PRI,colunms):
    MTU = 1000  #主键切片大小
    select_pri_sql = 'select {0} from {1}.{2};'.format(PRI,DB,TABLE)
    conn = src_db.db_connect(DB)
    pri = src_db.db_select(conn,select_pri_sql)
    pri = [ pri[i][0] for i in range(len(pri))]
    pri.sort()
    num = int(len(pri) / MTU)   #控制生成切片大小
    temp_pri = []       #临时存放主键
     
    for a in range(num + 1):
        startindex = a*MTU
        temp_pri = []           #初始化主键列表
        if a == num:            # 如果到了最后一轮，以防不足1000，最后索引就取总长度
            endindex =  len(pri)
        else:
            endindex = (a+1)*MTU 
        temp_pri = [ pri[i] for i in range(startindex,endindex) ]  #主键列表一次存储最多MTU个值                  
        tid = compare.delay(DB,TABLE,PRI,colunms,temp_pri)
    if tid.get():
        print("{0} finish.".format(TABLE))
            
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
            
            # 初始化redis,删除key
            rd = redisins.getins()
            errkeyname = "error-{0}-{1}".format(DB,TABLE)
            if rd.keys(errkeyname):
                rd.delete(errkeyname)
            okkeyname = "ok-{0}-{1}".format(DB,TABLE)
            if rd.keys(okkeyname):
                rd.delete(okkeyname)
                
            taskstart(SRCMYSQL,DB,TABLE,PRI,colunms)
            
            errkeyname = "error-{0}-{1}".format(DB,TABLE)
            errinfo = rd.lrange(errkeyname,0,-1)
            okkeyname = "ok-{0}-{1}".format(DB,TABLE)
            okinfo = rd.lrange(okkeyname,0,-1)
            
            errdict = {}
            errdict[errkeyname] = errinfo
            errdict = json.dumps(errdict)
            okdict = {}
            okdict[okkeyname] = okinfo
            okdict = json.dumps(okdict)
            
            with open(errlog,'a+') as f:
                f.write("{0}\n".format(errdict))
            f.close()
            
            with open(log,'a+') as f1:
                f1.write("{0}\n".format(okdict))
            f1.close()
             
                

if __name__ == '__main__':
    main()
