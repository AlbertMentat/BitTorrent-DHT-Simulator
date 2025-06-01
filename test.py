import heapq
import random
import logging
from collections import defaultdict, deque
from enum import IntEnum
from typing import List, Dict, Tuple

from basic import *
from simulator import Simulator

def generate_random_id() -> bytes:
    """生成随机160位ID"""
    return bytes([random.randint(0, 255) for _ in range(ID_LENGTH)])


def generate_ip_address(index: int) -> Tuple[str, int]:
    """生成IP地址"""
    return (f"192.168.{index // 256}.{index % 256}", 6881)


def main():
    """主函数：演示模拟器使用"""
    # 创建模拟器
    simulator = Simulator()

    # 添加种子节点
    seed_id = generate_random_id()
    seed_addr = generate_ip_address(1)
    simulator.add_seed_node(seed_id, seed_addr)

    # 安排节点加入事件
    num_nodes = 20
    for i in range(1, num_nodes + 1):
        node_id = generate_random_id()
        node_addr = generate_ip_address(100 + i)
        simulator.schedule_event(
            i * 5,  # 时间
            EventType.NODE_JOIN,
            {'node_id': node_id, 'address': node_addr}
        )

    # 创建一些文件
    num_files = 3
    files = [generate_random_id() for _ in range(num_files)]

    # 安排文件发布事件
    for i, file_id in enumerate(files):
        # 随机选择发布者
        publisher_idx = random.randint(1, num_nodes)
        publisher_id = list(simulator.nodes.values())[publisher_idx].node_id if publisher_idx < len(
            simulator.nodes) else seed_id

        simulator.schedule_event(
            100 + i * 20,
            EventType.FILE_SEED,
            {'node_id': publisher_id, 'file_id': file_id}
        )

    # 安排文件检索事件
    for i, file_id in enumerate(files):
        # 随机选择一个节点作为检索者
        retriever_idx = random.randint(1, num_nodes)
        retriever_id = list(simulator.nodes.values())[retriever_idx].node_id if retriever_idx < len(
            simulator.nodes) else seed_id

        simulator.schedule_event(
            200 + i * 30,
            EventType.FILE_RETRIEVE,
            {'node_id': retriever_id, 'file_id': file_id}
        )

    # 安排一些节点离开
    for i in range(3):
        leave_idx = random.randint(1, num_nodes)
        leave_id = list(simulator.nodes.values())[leave_idx].node_id if leave_idx < len(simulator.nodes) else None

        if leave_id:
            simulator.schedule_event(
                250 + i * 10,
                EventType.NODE_LEAVE,
                {'node_id': leave_id}
            )

    # 运行模拟
    simulator.run(max_ticks=400)

if __name__ == "__main__":
    # 设置随机种子以便复现结果
    random.seed(42)
    main()