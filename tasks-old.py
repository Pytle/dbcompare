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
            
            
def splitlist(LIST,length):
    '''
        传入一个列表，将列表切成以length为长度的子列表，然后列表返回。
    '''
    buffer_list = []
    for index in range(0,len(LIST),length):
        if index+length > len(LIST):
            endindex = len(LIST)
        else:
            endindex = index+length
        temp_list = [LIST[i] for i in range(index,endindex) ]
        buffer_list.append(temp_list)
    return buffer_list

    
def selector(db,table,priname,colstr,startpri,endpri,errlog):
    sql = 'select  concat_ws(\',\'{1})  from `{2}`.`{3}` where {4} >= \'{5}\' and {4} <= \'{6}\''.format(priname,colstr,db,table,priname,startpri,endpri)
    try:
        srcinfo = SRCMYSQL.db_select(SRCMYSQL.db_connect(db),sql)
        dstinfo = DSTMYSQL.db_select(DSTMYSQL.db_connect(db),sql)
        src = srcinfo[0][0]
        dst = dstinfo[0][0]       
    except Exception as e:
        return 255
        
        if src == dst:
            return 1      
        else :
            return 0
        
@app.task
def compare(db,table,priname,colunms,pri,log,errlog):
    if not pri:
        return 254
    diff_info = {}
    diff_info[db] = {}
    diff_info[db][table]={}
    colstr = '' 
    startpri = pri[0]
    endpri = pri[-1]
    # 拼接字段，生成sql语句
    for colunm in colunms:
        colstr = colstr + ',`{}`'.format(colunm)
    
    result = selector(db,table,priname,colstr,startpri,endpri,errlog)
    
    # 处理255情况
    if result == 255:
        # 逐项比较
        for primary in pri:
            try:
                r = selector(db,table,priname,colstr,primary,primary,errlog)
                if r == 1:
                    continue
                elif not r:
                    with open(errlog,'a+') as f1:
                        f1.write("{0}-{1}-{2} is not match\n".format(db,table,primary))
                else:
                    raise Exception("Empty result.")
            except Exception as e:
                with open(errlog,'a+') as f1:
                    f1.write("{0}-{1}-{2} error:{3}\n".format(db,table,primary,e))        
        return 3
        
        
    elif result == 1:
        with open(log,'a+') as f:
            f.write('ok:{0}-{1} {2}-{3},sql:{4}\n'.format(db,table,startpri,endpri,sql))
        return 0
        
    elif not result:
        MTU = 100
        result_list = splitlist(pri,MTU)
        for _list in result_list:
            startpri = _list[0]
            endpri = _list[-1]
            r = selector(db,table,priname,colstr,startpri,endpri,errlog)
            if r == 1 :
                continue
            else:
                # 逐项比较
                for primary in _list:
                    try:
                        _r = selector(db,table,priname,colstr,primary,primary,errlog)
                        if _r == 1:
                            continue
                        elif not _r:
                            with open(errlog,'a+') as f1:
                                f1.write("{0}-{1}-{2} is not match\n".format(db,table,primary))
                        else:
                            raise Exception("Empty result.")
                    except Exception as e:
                        with open(errlog,'a+') as f1:
                            f1.write("{0}-{1}-{2} error:{3}\n".format(db,table,primary,e))
        return 2
        
        else:
            print("unknow error.")
            return 255
        
        '''
        # 切片比较,查找不一致的数据。先每100个数据进行比较，如果比较发现不一致的数据，把100个数据逐个比较
        length = len(pri)
        MTU = 100
        num = int(length / MTU)
        for a in range(num) :
            startindex = a * MTU
            if a == num:            # 如果到了最后一轮，以防不足MTU值，最后索引就取总长度-1
                endindex = -1
            else:
                endindex = (a+1)*MTU - 1
            # 先比较100个
            try:
                startpri = pri[startindex]
            except Exception as e:
                print("length:%d" % length)
                print("startindex:%d" % startindex)
                
            try:
                endpri = pri[endindex]
            except Exception as e:
                print("length:%d" % length)
                print("endindex:%d" % endindex)
            print("startindex:%d" % startindex)
            print("endindex:%d" % endindex)
            src,dst,sql = selector(db,table,priname,colstr,startpri,endpri,errlog)            
            if src == dst:
                continue
            elif src != dst:
                for i in range(startindex,endindex):
                    # 逐项比较
                    src,dst,sql = selector(db,table,priname,colstr,pri[i],pri[i],errlog)
                    if src == dst:
                        continue
                    else:
                        with open(errlog,'a+') as f1:
                            f1.write("{0}-{1}-{2} is not match\n".format(db,table,pri[i]))        

        return 1
    '''
