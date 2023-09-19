from collections.abc import Callable, Iterable, Mapping
from typing import Any
from redis import Redis
from redis.client import Pipeline
from keycodec import PBKeyCodec
from test_pb2 import UserData,UserTaskAddReq,UserTaskAddRsp,UserTaskTakeReq,UserTaskTakeRsp
from keyproc import KeyProcess,PbOpBase
import threading
import sys
import time
import random

# 玩家任务进度达到10 就可以领奖了
config_task_award_count = 10

# 任务系统的消息队列
keyprocess:KeyProcess = None

# 玩家测试类
class PlayerTest(threading.Thread):
    class TaskAddRsp(PbOpBase):
        def __init__(self) -> None:
            super().__init__()
            self._PbType = UserTaskAddRsp

        def op_ok(self, keyprocess:KeyProcess):
            if self._ud.progress >= config_task_award_count:
                # 随机去领取
                if random.randint(0, 10) > 5:
                    ud = UserTaskTakeReq()
                    ud.userid = self._ud.userid
                    keyprocess.push_msg("task_take_req", self.encode(ud))
                    pass
                pass
            pass

        pass

    class TaskTakeRsp(PbOpBase):
        def __init__(self) -> None:
            super().__init__()
            self._PbType = UserTaskTakeRsp

        def op_ok(self, keyprocess:KeyProcess):
            print(f"userid{self._ud.userid} take award status{self._ud.status}")
            pass

        pass

    def run(self) -> None:
        # 注册
        global keyprocess
        keyprocess.register_cmd("task_add_rsp", PlayerTest.TaskAddRsp())
        keyprocess.register_cmd("task_take_rsp", PlayerTest.TaskTakeRsp())

        pbcodec = PBKeyCodec()
        # 测试用户ID
        playerlist = [user for user in range(100, 1000)]

        # 每隔一段时间添加一些玩家的任务进度,当任务进度完成后，进行领奖
        while True:
            time.sleep(0.5)

            n = random.randint(0, 100)
            if n < 10:
                print("req task add")
                req = UserTaskAddReq()
                req.userid = random.choice(playerlist)
                req.add = 10
                data = pbcodec.encode(req)
                keyprocess.push_msg("task_add_req", data)
                pass
            else:
                pass
            pass
        pass
    pass

# 任务系统
class TaskModule:
    class TaskAddOp(PbOpBase):
        def __init__(self) -> None:
            super().__init__()
            self._PbType = UserTaskAddReq
            self._res = None
            pass

        def get_keys(self):
            self._keys = []
            self._keys.append(TaskModule.to_key(self._ud.userid))
            return self._keys

        def op_func(self, keyprocess, pipeline: Pipeline, redis: Redis):
            key = TaskModule.to_key(self._ud.userid)
            data = redis.get(key)
            if data:
                self.encode()
                ud = UserData()
                self._codec.decode(data=data, stu=ud)
                pass
            else:
                ud = UserData()
                ud.userid = self._ud.userid
                ud.task_count = 0
                pass
            ud.task_count += self._ud.add
            self._res = ud
            data = self.encode(ud)
            pipeline.set(key, data)
            pass
        def op_ok(self, keyprocess:KeyProcess):
            # 推送rsp消息
            rsp = UserTaskAddRsp()
            rsp.userid = self._res.userid
            rsp.progress = self._res.task_count
            data = self.encode(rsp)
            keyprocess.push_msg("task_add_rsp", data)
            pass

    class TaskTakeOp(PbOpBase):
        def __init__(self) -> None:
            super().__init__()
            self._PbType = UserTaskAddReq
            self._status = 0

        def get_keys(self):
            self._keys = []
            self._keys.append(TaskModule.to_key(self._ud.userid))
            return self._keys
        
        def op_func(self, keyprocess, pipeline: Pipeline, redis: Redis):
            key = TaskModule.to_key(self._ud.userid)
            data = redis.get(key)
            if data:
                ud = self.decode(data, UserData)
                pass
            else:
                ud = UserData()
                ud.userid = self._ud.userid
                ud.task_count = 0
                pass
            if ud.task_count < config_task_award_count:
                # 不够领奖
                self._status = 1
                pass
            ud.task_count -= config_task_award_count
            data = self.encode(ud)
            pipeline.set(key, data)
            pass
        def op_ok(self, keyprocess:KeyProcess):
            rsp = UserTaskTakeRsp()
            rsp.userid = self._ud.userid
            rsp.status = self._status
            data = self.encode(rsp)
            keyprocess.push_msg("task_take_rsp", data=data)
            pass


    def __init__(self) -> None:
        global keyprocess
        keyprocess.register_cmd("task_add_req", TaskModule.TaskAddOp())
        keyprocess.register_cmd("task_take_req", TaskModule.TaskTakeOp())
        pass
    
    def to_key(userid):
        return f"task:{userid}"
    
    pass


def main(argv):
    # argv1: redis_url   redis.from_url 函数有说明示例
    # argv1: redis_url   redis.from_url 函数有说明示例
    global keyprocess
    keyprocess = KeyProcess(sys.argv[1], None, sys.argv[2])

    test = PlayerTest()
    test.start()

    task = TaskModule()

    keyprocess.process()



