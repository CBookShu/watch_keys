from redis import Redis
from redis.client import Pipeline
import pymysql

"""
主要就是通过redis缓存数据并进行第一手操作，然后慢慢把数据同步到mysql中
对于该数据一般来说就是序列化后的，mysql也仅仅作为一个kv硬盘来用

它的好处是，热数据基本全部都在redis上操作，冷数据会慢慢同步给mysql
基本性能是比较好的，同时由于冷数据会慢慢同步给mysql，针对于redis来说，不会无止境的占用内存

在对redis进行写入的时候，会进行额外的rds_key_dirty:key,进行写入


读通过Redis操作
写通过pipeline操作
就是接应了keywatch中的回调函数
"""


class KeyCache:
    _keys:list[str]
    _redis:Redis
    _pipeline:Pipeline

    def __init__(self, keys,redis,pipeline) -> None:
        self._keys = keys
        self._redis = redis
        self._pipeline = pipeline
        pass

    def get(self, key):
        return self._redis.get(key)

    def set(self, key, data):
        self._pipeline.set(key, data)
    pass
