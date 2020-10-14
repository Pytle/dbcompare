# dbcompare
使用celery异步任务模块，对比两个数据库的内容。

# 环境
centos7.4  
python3.7.4
celery5.0
mariadb5.6

# 用法
## 修改config.ini数据。
src_mysql 指源数据库
host 主机IP
user 数据库用户名
password 数据库密码
db 需要校验的库。多个库用","隔开
include 配置说明：all表示校验所有表 。检验特定几个表：表名用","隔开
exclude 配置说明：过滤特定表。表名用","隔开。优先级比include低

redis需要自行额外安装，建议使用docker安装：
```
	docker pull redis
```

```
	docker run --name myredis -d -p PORT:6379 redis --requirepass "PASSWORD"
```

## 服务器配置
安装依赖
```
pip install -r requirements.txt 
```

进入tasks.py目录，启动worker。如果使用多服务器消费，则在多个服务器上部署项目，启动即可。
```
celery -A tasks worker -l INFO
```

在其中一台服务器上执行run.py
```
python run.py
```

日志保存在log下。


# 联系作者
微信：jtandyx
