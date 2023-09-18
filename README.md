# watch_keys
    通过redis 的watch命令，可以进行一些原子操作（分布式）。
    这里用python 做个示例，通过redis缓存，mysql作为硬盘，来保存pb的二进制

# python 环境
    3.10.5 (没有特定要求，3.0以上应该就行吧)

# 安装包
    pip install pymysql 
    pip install redis 
    pip install google 
    pip install protobuf==3.20.0 

# protobuf
    sudo apt-get install protobuf-compiler
    windows 平台需要自行编译源码

    protoc ./test.proto --python_out=./

# 测试
    修改db和redis对应的ip,port,password等基础数据
    运行 pytest1.py 中的main函数

# todo
    1. redis和mysql的cache
    2. msglist 分区问题