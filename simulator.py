import heapq
import random
import logging
from collections import defaultdict, deque
from enum import IntEnum
from typing import List, Dict, Tuple

from basic import *
from node import Node


class Simulator:
    def __init__(self):
        self.current_time = 0
        self.nodes = {}  # node_id -> Node
        self.events = []  # 事件堆 (time, event_counter, event_type, params)
        self.event_counter = 0  # 事件计数器，确保事件顺序
        self.messages = defaultdict(list)  # time -> list of messages
        self.event_handlers = {
            EventType.NODE_JOIN: self.handle_node_join,
            EventType.NODE_LEAVE: self.handle_node_leave,
            EventType.FILE_SEED: self.handle_file_seed,
            EventType.FILE_RETRIEVE: self.handle_file_retrieve,
            EventType.MESSAGE: self.handle_message_delivery,
            EventType.BOOTSTRAP: self.handle_bootstrap_timeout  # 处理引导超时
        }
        self.log = []  # 模拟日志
        self.seed_node = None
        self.node_counter = 0

    def schedule_event(self, time: int, event_type: EventType, params: Dict):
        """安排事件"""
        # 使用事件计数器确保事件顺序
        heapq.heappush(self.events, (time, self.event_counter, event_type, params))
        self.event_counter += 1

    def schedule_message(self, delivery_time: int, message: Dict):
        """安排消息投递"""
        self.messages[delivery_time].append(message)

    def add_seed_node(self, node_id: bytes, address: Tuple[str, int]):
        """添加种子节点"""
        self.seed_node = (node_id, address)
        node = Node(node_id, address)
        node.state = NodeState.ACTIVE  # 种子节点直接激活
        self.nodes[node_id] = node
        logger.info(f"Seed node added: {node_id.hex()[:8]} at {address}")
        return node_id

    def handle_node_join(self, params: Dict):
        """处理节点加入事件"""
        node_id = params['node_id']
        address = params['address']

        if node_id in self.nodes:
            logger.warning(f"Node {node_id.hex()[:8]} already exists")
            return

        # 创建新节点
        node = Node(node_id, address)
        self.nodes[node_id] = node
        self.node_counter += 1
        logger.info(f"Node joined: {node_id.hex()[:8]} at {address} (Total nodes: {self.node_counter})")

        # 如果存在种子节点，开始引导过程
        if self.seed_node:
            seed_id, seed_addr = self.seed_node
            node.start_bootstrapping(seed_id, seed_addr, self.current_time, self)

    def handle_bootstrap_timeout(self, params: Dict):
        """处理引导超时事件"""
        node_id = params['node_id']

        if node_id in self.nodes:
            node = self.nodes[node_id]
            if node.state == NodeState.BOOTSTRAPPING:
                logger.warning(f"Node {node_id.hex()[:8]} bootstrap timed out")
                node.complete_bootstrapping(self.current_time, self)

    def handle_node_leave(self, params: Dict):
        """处理节点离开事件"""
        node_id = params['node_id']

        if node_id in self.nodes:
            del self.nodes[node_id]
            self.node_counter -= 1
            logger.info(f"Node left: {node_id.hex()[:8]} (Total nodes: {self.node_counter})")
        else:
            logger.warning(f"Node not found: {node_id.hex()[:8]}")

    def handle_file_seed(self, params: Dict):
        """处理文件发布事件"""
        node_id = params['node_id']
        info_hash = params['file_id']

        if node_id not in self.nodes:
            logger.warning(f"Node not found: {node_id.hex()[:8]}")
            return

        node = self.nodes[node_id]

        # 查找最近的节点
        closest_nodes = node.get_closest_nodes(info_hash)
        logger.debug(f"Found {len(closest_nodes)} closest nodes for file {info_hash.hex()[:8]}")

        # 向最近的节点发送STORE请求
        store_count = 0
        for nid, addr in closest_nodes:
            if nid in self.nodes and nid != node_id:  # 不向自己发送
                store_msg = {
                    'type': MessageType.STORE,
                    'sender_id': node_id,
                    'sender_addr': node.address,
                    'recipient_id': nid,
                    'recipient_addr': addr,
                    'data': {
                        'info_hash': info_hash,
                        'provider_address': node.address
                    }
                }
                self.schedule_message(self.current_time + 1, store_msg)
                store_count += 1

        logger.info(f"Node {node_id.hex()[:8]} seeding file {info_hash.hex()[:8]} to {store_count} nodes")

        # 安排重新发布
        self.schedule_event(
            self.current_time + REPUBLISH_INTERVAL,
            EventType.FILE_SEED,
            params
        )

    def handle_file_retrieve(self, params: Dict):
        """处理文件检索事件"""
        node_id = params['node_id']
        info_hash = params['file_id']

        if node_id not in self.nodes:
            logger.warning(f"Node not found: {node_id.hex()[:8]}")
            return

        node = self.nodes[node_id]

        # 查找最近的节点
        closest_nodes = node.get_closest_nodes(info_hash)
        logger.debug(f"Found {len(closest_nodes)} closest nodes for file {info_hash.hex()[:8]}")

        # 向最近的节点发送FIND_VALUE请求
        request_count = 0
        for nid, addr in closest_nodes:
            if nid in self.nodes and nid != node_id:  # 不向自己发送
                find_value_msg = {
                    'type': MessageType.FIND_VALUE,
                    'sender_id': node_id,
                    'sender_addr': node.address,
                    'recipient_id': nid,
                    'recipient_addr': addr,
                    'data': {'info_hash': info_hash}
                }
                self.schedule_message(self.current_time + 1, find_value_msg)
                request_count += 1

        logger.info(f"Node {node_id.hex()[:8]} retrieving file {info_hash.hex()[:8]} from {request_count} nodes")

    def handle_message_delivery(self, params: Dict):
        """处理消息投递事件"""
        message = params['message']
        recipient_id = message['recipient_id']

        if recipient_id in self.nodes:
            node = self.nodes[recipient_id]
            node.message_queue.append(message)
            logger.debug(f"Message delivered to {recipient_id.hex()[:8]}")
        else:
            logger.warning(f"Target node not found for message: {recipient_id.hex()[:8]}")

    def process_messages(self):
        """处理当前时间的所有消息"""
        if self.current_time in self.messages:
            for msg in self.messages[self.current_time]:
                # 创建消息投递事件
                self.schedule_event(
                    self.current_time,
                    EventType.MESSAGE,
                    {'message': msg}
                )

            # 移除已处理的消息
            del self.messages[self.current_time]

    def run(self, max_ticks: int = 1000):
        """运行模拟"""
        logger.info(f"Starting simulation for {max_ticks} ticks")

        while self.current_time <= max_ticks and (self.events or self.messages):
            # 处理当前时间的消息
            self.process_messages()

            # 处理所有当前时间的事件
            while self.events and self.events[0][0] <= self.current_time:
                time, counter, event_type, params = heapq.heappop(self.events)
                self.event_handlers[event_type](params)

            # 处理所有节点的消息队列
            for node in list(self.nodes.values()):
                while node.message_queue:
                    msg = node.message_queue.popleft()
                    node.handle_message(msg, self.current_time, self)

            # 每50个时间单位记录一次状态
            if self.current_time % 50 == 0:
                self.log_state()

            # 推进时间
            self.current_time += 1

        logger.info(f"Simulation completed at tick {self.current_time}")
        self.dump_state()

    def log_state(self):
        """记录当前状态"""
        # 统计节点状态
        state_counts = defaultdict(int)
        for node in self.nodes.values():
            state_counts[node.state] += 1

        logger.info(f"===== Time: {self.current_time} =====")
        logger.info(f"Total nodes: {self.node_counter}")
        logger.info(f"Node states: {dict(state_counts)}")
        pending_messages = sum(len(msgs) for msgs in self.messages.values())
        logger.info(f"Pending messages: {pending_messages}")
        logger.info(f"Pending events: {len(self.events)}")

    def dump_state(self):
        """输出最终状态"""
        logger.info("===== DHT Network Final State =====")
        logger.info(f"Current time: {self.current_time}")
        logger.info(f"Total nodes: {self.node_counter}")

        for node_id, node in self.nodes.items():
            logger.info(f"\nNode: {node_id.hex()[:8]} at {node.address} (State: {node.state.name})")

            # 输出K桶信息
            logger.info("K-Buckets:")
            non_empty_buckets = 0
            for i, bucket in enumerate(node.k_buckets):
                if bucket:
                    non_empty_buckets += 1
                    bucket_info = ", ".join([nid.hex()[:4] for nid, _, _ in bucket])
                    logger.info(f"  Bucket {i}: [{bucket_info}]")
            logger.info(f"Total non-empty buckets: {non_empty_buckets}")

            # 输出存储的文件信息
            logger.info("Stored Files:")
            for info_hash, providers in node.storage.items():
                provider_addrs = ", ".join([f"{ip}:{port}" for ip, port in providers])
                logger.info(f"  File {info_hash.hex()[:8]} from [{provider_addrs}]")