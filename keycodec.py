import json

"""
数据序列化和反序列化的类，基础使用的是pb接口
"""

class KeyCodec:

    def __init__(self) -> None:
        pass

    # 序列化成字符串或者bytes
    def encode(self,stu:any)->bytes:
        return ""

    # 反序列化成数据结构
    def decode(self, data:bytes, stu:any = None) -> any:
        return None

    pass


# protobuf 类型的解析
class PBKeyCodec(KeyCodec):

    def encode(self, stu: any) -> bytes:
        return stu.SerializeToString()
    
    def decode(self, data: bytes, stu:any) -> any:
        stu.ParseFromString(data)
        return stu

    pass

# json 类型的解析
class JsonKeyCodec(KeyCodec):

    def encode(self, stu: any) -> bytes:
        return json.dumps(stu)
    
    def decode(self, data: bytes) -> any:
        return json.loads(data)
    
    pass


if __name__ == "__main__":
    from test_pb2 import UserData

    # protobuf 测试
    ud = UserData()
    ud.userid = 1111
    pbkc = PBKeyCodec()
    data = pbkc.encode(ud)
    ud1 = UserData()
    assert(ud == pbkc.decode(data, ud1))

    # json 测试
    jsud = {
        "userid":1111
    }
    jskc = JsonKeyCodec()
    data = jskc.encode(jsud)
    assert(jsud == jskc.decode(data))

    pass