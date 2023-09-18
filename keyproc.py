
from keycache import KeyCache
from keywatch import WatchKeys
from keycodec import KeyCodec,JsonKeyCodec,PBKeyCodec
from rdsmsglist import KeyMsgList
from redis import Redis
from redis.client import Pipeline
import base64
import traceback

"""
watch keys -> process -> cache
分布式的简单数据库业务中间件
"""

class OpBase:
    def __init__(self) -> None:
        self._msg = None
        self._keys = []
        self._ud = None
        self._keywatch = None
        pass

    # 需要用户自己实现 用于填充 self._ud
    def parse_data(self, bdata:bytes):
        return self._ud

    # 需要用户自己实现 用于填充 self._keys
    def get_keys(self):
        return self._keys
    
    # 需要用户自己实现
    def op_func(self,keyprocess,pipeline:Pipeline,redis:Redis):
        pass

    # 如果操作成功，调用该接口
    def op_ok(self,keyprocess):
        pass
    pass

class KeyProcess:
    _keywatch:WatchKeys
    _trycount:int
    _msglist:KeyMsgList
    _msgcodec:JsonKeyCodec

    """
    _msgopmap = {"cmd":OpBase}
    """
    _msgopmap:{}

    def __init__(self,rdsurl,msqlurl,mqurl) -> None:
        self._keywatch = WatchKeys(rdsurl)
        self._trycount = 1 # 仅一次
        self._msglist = KeyMsgList(mqurl)
        self._msgcodec = JsonKeyCodec()
        self._msgopmap = {}
        self._running = False
        pass
    
    # 自定义解析格式begin
    def encode_msg(self, msg:dict):
        # 默认是json
        return self._msgcodec.encode(msg)
    
    def decode_msg(self, data:bytes):
        # 默认是json
        return self._msgcodec.decode(data=data)
    
    def set_coedc(self,codec:KeyCodec):
        self._msgcodec = codec
    # 自定义解析格式end

    def set_trycount(self,count:int):
        self._trycount = count
    
    def get_keywatch(self):
        return self._keywatch
    
    def get_msglist(self):
        return self._msglist

    def register_cmd(self, cmd:str, op:OpBase):
        self._msgopmap[cmd] = op
        pass

    def push_msg(self,cmd:str, data:bytes = None, trycount:int = 1):
        msg = {
            "cmd":cmd,
            "trycount":trycount,
        }
        if data:
            msg["data"] = str(base64.encodebytes(data), "utf-8")
            pass
        jdata = self.encode_msg(msg=msg)
        self._msglist.push("msg", msg=jdata)

    def _opcenter(self, msg:dict):
        """
        msg = {
            cmd:str
            data:base64,
            trycount:int
        }
        """
        try:
            cmd = msg["cmd"]
            data = msg.get("data", "")
            data = bytes(data, "utf-8")
            trycount = msg.get("trycount", self._trycount)
            b_data = base64.decodebytes(data)

            op:OpBase = self._msgopmap[cmd]
            ud = op.parse_data(bdata=b_data)
            keys = op.get_keys()
            # opfunc = def (pipeline, redis)
            def _f(pipeline, redis):
                op.op_func(self,pipeline=pipeline, redis=redis)
                pass
            op_ok = False
            for _ in range(0, trycount):
                if self._keywatch.watch_keys(keys=keys, cb = _f):
                    op_ok = True
                    break
                pass
            if op_ok:
                op.op_ok(self)
            pass
        except :
            traceback.print_exc()
            pass

    def process(self):
        self._running = True
        while self._running:
            print("wait msg")
            b_msg = self._msglist.b_peek("msg")
            if b_msg is None:
                continue
            try:
                msg = self.decode_msg(b_msg)
                self._opcenter(msg)
                pass
            except Exception as e:
                print(b_msg)
                # print(f"what exception:{type(e)}, msg:{e}")
                traceback.print_exc()
                pass
            finally:
                self._msglist.consume("msg",b_msg)    
            pass
        pass

    pass

# 一些基础的消息处理
class StopOp(OpBase):
    def __init__(self) -> None:
        super().__init__()
    
    def op_func(self, keyprocess: KeyProcess, pipeline: Pipeline, redis: Redis):
        keyprocess._running = False
        print("keyprocess stop")
    pass

# 简化的基础类
class PbOpBase(OpBase):
    def __init__(self) -> None:
        super().__init__()
        self._PbType = None
        self._codec = PBKeyCodec()

    def parse_data(self, bdata: bytes):
        self._ud = self.decode(bdata, self._PbType)
        return self._ud

    def decode(self,bdata:bytes,pb):
        ud = pb()
        self._codec.decode(bdata, ud)
        return ud

    def encode(self, stu):
        return self._codec.encode(stu=stu)

    pass
