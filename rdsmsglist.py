from redis import Redis


"""
list 实现，最简单，可以通过RPOPLPUSH 的方式，产生ack的流程
订阅/发布  由于无法ack 它非常不安全
Stream: 需要高版本的redis，而且还不熟。。。
"""

class KeyMsgList:
    _redis:Redis

    def __init__(self,rdsurl) -> None:
        self._redis = Redis.from_url(rdsurl)
        pass
    
    def push(self,q:str, msg:bytes):
        self._redis.rpush(q, msg)
        pass

    # 返回待处理的消息
    def peek(self, q:str):
        q_back = self._backqueuename(q)
        # 先处理缓存队列中未完成的数据
        msg = self._redis.lindex(q_back, 0)
        if msg is not None:
            return msg
        
        # 对消息队列进行
        msg = self._redis.rpoplpush(q, q_back)
        return msg
    
    # 返回待处理的消息，无消息进行等待
    # t 不传将无限等待
    def b_peek(self, q:str, t:int = None):
        q_back = self._backqueuename(q)
        # 先处理缓存队列中未完成的数据
        msg = self._redis.lindex(q_back, 0)
        if msg is not None:
            return msg
        
        # 对消息队列进行 并阻塞
        msg = self._redis.brpoplpush(q, q_back, t)
        return msg
    
    def consume(self, q:str, msg:bytes):
        q_back = self._backqueuename(q)
        self._redis.lrem(q_back,1, msg)
        pass

    def _backqueuename(self, q:str):
        return f"{q}_back"
    pass
