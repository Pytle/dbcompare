#-- coding:utf-8 --
from lib.redis import Redis
from lib.mysql import Mysql
from lib.rabbitmq import Rabbitmq
import configparser
import os
import json
import time
import queue
from celery import Celery

app = Celery('producer',broker='redis://:jttest123@106.53.21.131:4399/0',backend='redis://:jttest123@106.53.21.131:4399/0')

app.task
def test(x,y):
    time.sleep(3)
    return x + y