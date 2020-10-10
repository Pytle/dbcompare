# coding:utf-8
import os
import json
import time



# 此类主要封装了comparetable方法，用于对比一定范围内的数据是否一致，输出日志
class Pri_key_consumer():
    def __init__(self,SRC_MYSQL,DST_MYSQL):
        self.SRCMYSQL = SRC_MYSQL
        self.DSTMYSQL = DST_MYSQL

    def comparetable(self,db,table,priname,colunms,pri):
        if not pri:
            return 0
        basedir = os.path.dirname(os.path.abspath(__file__))
        logdir = os.path.join(basedir,'log')
        log = os.path.join(logdir,'result.log')
        errlog = os.path.join(logdir,'err.log')
        diff_info = {}
        diff_info[db] = {}
        diff_info[db][table]={}
        colstr = '' 
        startpri = pri[0]
        endpri = pri[-1]
        # 拼接字段，生成sql语句
        for colunm in colunms:
            colstr = colstr + ',`{}`'.format(colunm)
        sql = 'select  concat_ws(\',\'{1})  from `{2}`.`{3}` where {4} > \'{5}\' and {4} < \'{6}\''.format(priname,colstr,db,table,priname,startpri,endpri)
        try:
            src = self.SRCMYSQL.db_select(self.SRCMYSQL.db_connect(db),sql)[0][0]
            dst = self.DSTMYSQL.db_select(self.DSTMYSQL.db_connect(db),sql)[0][0]
        except Exception as e:
            print(self.SRCMYSQL.db_select(self.SRCMYSQL.db_connect(db),sql))
            return 0
        if src == dst:
            with open(log,'a+') as f:
                f.write('ok:{0}-{1}-{2},sql:{3}\n'.format(db,table,startpri,sql))
            return 0
        else:
            with open(errlog,'a+') as f1:
                f1.write("{0}-{1}-{2} is not match".format(db,table,startpri"-"endpri))
            return 1
        
            
