import pymysql
import redis
from redis.client import Pipeline
from redis.exceptions import WatchError

_mysql = None
_redis = None

class Entry:
    _pipeline:Pipeline = None
    _keys = []

    def __init__(self, pipeline, keys) -> None:
        self._pipeline = pipeline
        self._keys = keys
        pass
    
    def getcache(self, mainkey, pbstu):
        key = _rdskey(mainkey)
        self._keys.index(key)       # check or exception

        global _redis
        data = _redis.get(key)
        if pbstu and data :
            pbstu.ParseFromString(data)
        return data

    def setcache(self, mainkey, pbstu):
        key = _rdskey(mainkey)
        self._keys.index(key)       # check or exception

        data = pbstu.SerializeToString()
        self._pipeline.set(key, data)
    pass

class MysqlConInfo:
    host:str
    user:str
    port:int
    password:str
    database:str

    def __init__(self, host:str,user:str,port:int,password:str,database:str) -> None:
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.database = database
        pass
    pass

class RedisConInfo:
    host:str
    port:int
    password:str
    dbindex:int
    
    def __init__(self, host:str,port:int,password:str,dbindex:int) -> None:
        self.host = host
        self.port = port
        self.password = password
        self.dbindex = dbindex
        pass
    pass

def init(db:MysqlConInfo,rds:RedisConInfo):
    try:
        # mysql 连接
        global _mysql
        if db:
            _mysql = pymysql.connect(host=db.host, user=db.user, passwd=db.password, port=db.port, database=db.database)
    
        # redis 连接
        global _redis
        _redis = redis.Redis(host=rds.host, port=rds.port,password=rds.password)
        _redis.select(rds.dbindex)
        pass
    except Exception as e:
        print(e)
        # 重新抛出去，让外部报错
        raise e

    return True

def _rdskey(mainkey):
    return f"rds_as_key:{mainkey}"

def watch(keys:list, cb):
    # 生成redis的表结构
    rds_keys = []
    for key in keys:
        rds_key = _rdskey(key)
        rds_keys.append(rds_key)
        pass
    
    global _redis
    try:
        pipe = _redis.pipeline()
        pipe.watch(*rds_keys)
        pipe.multi()
        if cb is not None:
            entry = Entry(pipe, rds_keys)
            cb(entry)
        pipe.execute()
        pass
    except WatchError as e:
        if e.args[0] == "Watched variable changed.":
            return False
        raise e
    finally:
        pipe.unwatch()
        pass
    return True

def createsql(name):
    pass


# 测试用例
if __name__ == "__main__":
    import lasyncache
    from lasyncache import Entry
    from test_pb2 import UserData

    ud = UserData()
    ud.userid = 123456

    lasyncache.init(
        MysqlConInfo(host="192.168.0.1", user="test", port=3306, password="test", database="test"), 
        RedisConInfo(host="192.168.0.1", port=6379, password="test", dbindex=1))
    
    def lf(entry:Entry):
        ud_r = UserData()
        entry.getcache("test1", ud_r)
        entry.setcache("test2", ud)
        pass
    ok = lasyncache.watch(["test1", "test2"], lf)
    pass
