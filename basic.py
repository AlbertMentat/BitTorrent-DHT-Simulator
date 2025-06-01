from enum import IntEnum
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('DHT_Simulator')

# 定义常量
ID_LENGTH = 20  # 160位ID (20字节)
K = 8  # K桶大小
ALPHA = 3  # 并发请求数
TICK_DURATION = 100  # 每个时间单位代表100ms
REPUBLISH_INTERVAL = 500  # 重新发布文件的间隔
BOOTSTRAP_TIMEOUT = 10  # 引导过程超时时间

class EventType(IntEnum):
    NODE_JOIN = 1
    NODE_LEAVE = 2
    FILE_SEED = 3
    FILE_RETRIEVE = 4
    MESSAGE = 5
    BOOTSTRAP = 6  # 新增引导过程事件


class MessageType(IntEnum):
    PING = 1
    FIND_NODE = 2
    FIND_VALUE = 3
    STORE = 4
    RESPONSE = 5


class NodeState(IntEnum):
    INIT = 0
    BOOTSTRAPPING = 1
    ACTIVE = 2
    LEAVING = 3