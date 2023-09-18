import redis
from redis.client import Pipeline
from redis.exceptions import WatchError

"""
通过redis的watch命令，支持多连接的原子操作
实际上就是分布式的自旋操作
"""

class WatchKeys:
    _redis:redis.Redis

    # url 格式查看 Redis.from_url
    # redis://[[username]:[password]]@localhost:6379/0
    # rediss://[[username]:[password]]@localhost:6379/0
    # unix://[username@]/path/to/socket.sock?db=0[&password=password]
    def __init__(self, url:str) -> None:
        self._redis = redis.from_url(url)
        pass

    # 监听key 并进行操作
    # return: True 操作成功, False 操作过程key有修改，其他抛出异常
    def watch_keys(self, keys:[str], cb:callable):
        try:
            pipe = self._redis.pipeline()
            if len(keys) > 0:
                pipe.watch(*keys)
                pipe.multi()
                cb(pipe, self._redis)
                pipe.execute()
                pass
            else:
                cb(pipe, self._redis)
                pass
            pass
        except WatchError as e:
            if e.args[0] == "Watched variable changed.":
                return False
            raise e
        finally:
            pipe.unwatch()
        pass
        return True
    pass
