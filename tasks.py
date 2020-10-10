# coding:utf-8
from celery import Celery
import os
import json
import time
import configparser

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
        
# 调用传过来的对象的comparetable方法，必须是Pri_key_consumer对象
@app.task
def compare(db,table,priname,colunms,pri,conobj):
    conobj.comparetable(db,table,priname,colunms,pri)
            
