import redis

# redis实例
class Redis():
    def __init__(self,host,port,password):
        self.host = host
        self.port = port
        self.password = password
        try:
            self.r = redis.Redis(host=self.host, port=self.port, password=self.password, decode_responses=True)
            print("Redis connect success...")
        except Exception as e:
            print("Redis 连接失败：",e)

    def str_get(self, k ,time=None):
        return self.r.get(k)


    def str_set(self, k ,v, time=None):
        self.r.set(k, v, time)

    def delete(self, k):
        tag = self.r.exists(k) #判断这个Key是否存在
        if tag:
            self.r.delete(k)
            print('删除成功')
        else:
            print('这个key不存在')
    
    def hash_hget(self, name, key):
        res = self.r.hget(name, key)
        if res:
            return res.decode()

    def hash_hset(self,name, k, v):
        self.r.hset(name, k, v)

    def hash_getall(self, name):
        res = self.r.hgetall()
        new_dict = {}
        if res:
            for k, v in res.items():
                k = k.decode()
                v = v.decode()
                new_dict[k] = v
        return new_dict

    def hash_del(self, name,k):
        res = self.r.hdel(name, k)
        if res:
            print('删除成功')
            return True
        else:
            print('删除失败.该key不存在')
            return False

    @property
    def clean_redis(self):
        self.r.flushdb() #清空redis
        print('清空redis成功.')
        return 0