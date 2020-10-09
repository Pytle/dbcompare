from celery import Celery
import os
import json
import time

app = Celery('producer',broker='redis://:jttest123@106.53.21.131:4399/0',backend='redis://:jttest123@106.53.21.131:4399/0')

class test():
	@app.task
	def test(x,y):
    		time.sleep(3)
    		return x + y


# 从队列中读取主键，redis中读取库、表和字段生成sql进行比较。结果回写到redis。主键消费者
class Pri_key_consumer():
    def __init__(self,RD,k,SRC_MYSQL,DST_MYSQL):
        self.RD = RD
        self.k = k
        self.SRCMYSQL = SRC_MYSQL
        self.DSTMYSQL = DST_MYSQL

    def __get_db_tables(self):
        res = self.RD.str_get(self.k)
        return json.loads(res)

    @app.task
    def comparetable(self,db,table,priname,colunms,pri):
        if not pri:
            return 0
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
        sql = 'select  concat_ws(\',\'{1})  from `{2}`.`{3}` where {4} > \'{5}\' and {4} < \'{6}\''.format(priname,colstr,db,table,priname,startpri,endpri)
        src = self.SRCMYSQL.db_select(self.SRCMYSQL.db_connect(db),sql)[0][0]
        dst = self.DSTMYSQL.db_select(self.DSTMYSQL.db_connect(db),sql)[0][0]
        if src == dst:
            # with open(file,'a+') as f:
            #     f.write('diffprikey:{0}-{1}-{2},sql:{3}\n'.format(db,table,pri,sql))
            print('diffprikey:{0}-{1}-{2},sql:{3}\n'.format(db,table,pri,sql))
            return 0
        else:
            pass
